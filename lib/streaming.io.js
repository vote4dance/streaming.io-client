(function (root) {
    var Streaming = {
        subscriptions: {},
        callbacks: {},
    };

    Streaming.NullDbDriver = function () {
        this.initialize = function initialize() {
            return new Promise(function (resolve) {
                resolve();
            });
        };

        /**
         *
         * @param url - What to retrieve from the database
         * @returns {Promise}
         *
         * Promise resolves into the following structure:
         * {
         *  data: data
         *  revision: what revision the data is
         *  hash: Hash of the data for comparison
         *  expire: when the data expires
         *  user_id: who stored the data
         * }
         *
         */
        this.get = function (url) {
            return new Promise(function get(resolve, reject) {
                reject(new Error("No driver configured"));
            });
        };

        /**
         *
         * @param url - What to store
         * @param data - Data to store
         * @param meta - Metadata describing the data
         * @returns {Promise}
         *
         * Meta structure should look as follows:
         * {
         *  revision: Revision of the currently retrieved data (undefined if none has been previously been retrieved)
         *  hash: Hash of the data
         *  expire: When the data should expire
         *  user_id: User id of whoever is storing the data
         * }
         *
         * Promise resolves into the revision of the updated data
         */
        this.put = function (url, data, meta) {
            return new Promise(function put(resolve, reject) {
                reject(new Error("No driver configured"));
            });
        };
    };

    Streaming.setUser = function setUser(user_id) {
        this.user_id = user_id;
    };

    Streaming.setup = function setup(socket, options) {
        options = options || {};

        _.defaults(options, {
            expire: 2 * 60 * 60 * 1000
        });

        this.socket = socket;
        this.options = options;

        this.user_id = 0;
        this.db = new (options.db ? options.db : this.NullDbDriver)();

        this.db.initialize().then(function () {}, function (err) {
            console.error("Failed to initialize DB driver", err);
        });

        socket.on('connect', _.bind(function() {
            this.onConnect(socket);
        }, this));

        socket.on('disconnect', _.bind(function () {
            this.onDisconnect();
        }, this));

        socket.on('stream', _.bind(function (message) {
            this.sync(message);
        }, this));
    };

    Streaming.sync = function sync(message) {
        var item = this.subscriptions[message.url];
        if (!item) {
            return;
        }

        this.db.put(message.url, message.data, {
            revision: item.revision,
            hash: message.hash,
            user_id: this.user_id,
            expire: new Date(new Date().getTime() + this.options.expire)
        }).then(function (revision) {
            item.revision = revision;
        });

        var data = item.cache = this.uncompress(message.data);
        try {
            this.apply(data, item.clients);
        } catch (e) {
            console.error(e.stack ? e.stack : e.message);
        }
    };

    Streaming.apply = function apply(data, items) {
        _.each(items, function (model) {
            if (!model.streaming) {
                return;
            }

            var initial = model.streaming.options.initial;
            model.streaming.options.initial = false;

            data = model.parse(data);

            if (model.models.length > 0) {
                var idAttribute = model.models[0].idAttribute;
                var equals = false;
                do {
                    if (initial === true) {
                        break;
                    }

                    if (model.models.length != data.length) {
                        break;
                    }

                    var local = _.pluck(model.models, "id");
                    var remote = _.pluck(data, idAttribute)

                    var i = 0, n = model.models.length;
                    for (;i !=n; ++i) {
                        if (local[i] != remote[i]) {
                            break;
                        }
                    }
                    if (i != n) {
                        break;
                    }
                    equals = true;
                } while (0);
                if (equals) {
                    model.forEach(function (current, index) {
                        current.set(current.parse(data[index]));
                    });
                    model.trigger('sync', model, data, {});
                } else {
                    model.reset(data);
                }

            } else {
                model.reset(data);
            }
        });
    };

    Streaming.emit = function emit(url, item) {
        item.updating = true;

        var message = {
            url: url
        };

        this.db.get(url)
            .then(_.bind(function (doc) {
                item.revision = doc.revision;

                if ((doc.user_id == this.user_id) && item.options.precache) {
                    message.hash = doc.hash;
                    item.cache = this.uncompress(doc.data);
                }

                try {
                    this.apply(item.cache, item.clients);
                } catch (e) {
                    console.error(e.stack ? e.stack : e.message);
                }

                return doc;
            }, this), function (err) {})
            .then(_.bind(function (doc) {
                this.send("stream", message, _.bind(function (err, response) {
                    delete item.updating;

                    if (err) {
                        return;
                    }

                    var uncompressed;
                    if (response.hash == message.hash) {
                        var limit = Date.now() + this.options.expire / 2;
                        if (doc.expire < limit || (doc.user_id != this.user_id)) {
                            this.db.put(message.url, doc.data, {
                                revision: item.revision,
                                hash: message.hash,
                                user_id: this.user_id,
                                expire: Date.now() + this.options.expire
                            }).then(function (revision) {
                                item.revision = revision;
                            }, function (err) {
                                console.error(err);
                            });
                        }

                        if (!item.cache) {
                            item.cache = uncompressed = this.uncompress(doc.data);
                        }
                    } else {
                        this.db.put(message.url, response.data, {
                            revision: item.revision,
                            hash: response.hash,
                            user_id: this.user_id,
                            expire: Date.now() + this.options.expire
                        }).then(function (revision) {
                            item.revision = revision;
                        }, function (err) {
                            console.error(err);
                        });

                        item.cache = uncompressed = this.uncompress(response.data);
                    }

                    if (uncompressed) {
                        try {
                            this.apply(uncompressed, item.clients);
                        } catch (e) {
                            console.error(e.stack ? e.stack : e.message);
                        }
                    }
                }, this));
            }, this));
    };

    Streaming.onConnect = function onConnect(socket) {
        this.socket = socket;

        _.each(this.subscriptions, function (item, url) {
            this.emit(url, item);
        }, this);
    };

    Streaming.onDisconnect = function onDisconnect() {
        _.each(this.subscriptions, function (item) {
            delete item.cache;
            if (item.timeout) {
                clearTimeout(item.timeout);
                delete item.timeout;
            }
        });

        _.each(this.callbacks, function (callback, id) {
            callback("Disconnected");
        });
        this.callbacks = {};

        delete this.socket;
    };

    Streaming.add = function add(target, options) {
        var url = target.url();
        if (target.streaming) {
            return;
        }

        options = options ? _.clone(options) : {};
        _.defaults(options, {
            "precache": !!target.precache,
            "initial": true
        });

        var item = this.subscriptions[url];
        if (!_.isUndefined(item)) {
            if (item.timeout) {
                clearTimeout(item.timeout);
                delete item.timeout;
            }

            target.streaming = { url: url, options: options };
            item.clients[target.cid] = target;

            if (item.cache) {
                try {
                    this.apply(item.cache, [target]);
                } catch (e) {
                    console.error(e.stack ? e.stack : e.message);
                }
            }
            return;
        } else {
            item = { clients: {}, options: options };
            item.clients[target.cid] = target;

            target.streaming = { url: url, options: options };
            this.subscriptions[url] = item;
        }

        if (_.isUndefined(this.socket)) {
            return;
        }

        this.emit(url, item);
    };

    Streaming.remove = function remove(target) {
        if (target.streaming) {
            var url = target.streaming.url;
            var item = this.subscriptions[url];

            delete item.clients[target.cid];
            if (_.keys(item.clients).length == 0) {
                item.timeout = setTimeout(_.bind(function () {
                    this.socket.emit("unstream", { url: url }, function (err) {});
                    delete this.subscriptions[url];
                }, this), 5000);
            }

            delete target.streaming;
        }
    };

    Streaming.send = function send(type, message, callback) {
        if (!this.socket) {
            _.defer(function () {
                callback("No connection");
            });
            return;
        }

        var id = _.uniqueId('cb');
        this.callbacks[id] = callback;

        this.socket.emit(type, message, _.bind(function (err, response) {
            delete this.callbacks[id];
            callback(err, this.uncompress(response));
        }, this));
    };

    Streaming.uncompress = function (data) {
        if (!_.isObject(data)) {
            return data;
        }

        var objectKeys = _.keys(data);

        if ((objectKeys.length != 2) || (_.without(objectKeys, 'keys', 'rows').length != 0)) {
            return data;
        }

        var keys = data.keys;
        var uncompressed = _.map(data.rows, function (row) {
            return _.object(keys, row);
        });

        return uncompressed;
    };

    var sync = function sync(method, model, options) {
        var message = {
            method: method,
            url: model.url(),
            cid: model.cid,
            data: options.arguments || model.toJSON() || {},
            emit: options.emit
        };

        Streaming.send("sync", message, function (err, response) {
            if (err) {
                options.error(err);
            } else {
                options.success(response);
            }
        });
    };

    Streaming.Model = Backbone.Model.extend({
        stream: function stream() {
            console.trace();
            alert("Streaming models is not currently supported");
        },

        unstream: function unstream() {},

        incrementVersion: function incrementVersion() {
            var versionName = this.versionAttribute;
            if (!_.isUndefined(versionName)) {
                this.set(versionName, this.parseVersionNumber(this.get(versionName)) + 1, {silent: true});
            }
        },

        parseVersionNumber: function parseVersionNumber(versionNumber) {
            return _.isFinite(versionNumber) ? versionNumber : 0;
        },

        parse: function parse(resp) {
            var versionName = this.versionAttribute;
            if (_.isObject(resp) && !_.isUndefined(versionName)) {
                var localVersion = this.parseVersionNumber(this.get(versionName));
                var remoteVersion = this.parseVersionNumber(resp[versionName]);

                if (localVersion > remoteVersion) {
                    return {};
                }
            }
            return resp;
        },

        emit: function emit(method, args, options) {
            if (_.isUndefined(options)) {
                options = args ? _.clone(args) : {};
                args = undefined;
            } else {
                options = options ? _.clone(options) : {};
            }

            var model = this;

            var triggerEmit = _.bind(function() {
                model.trigger('emit emit:' + method, model, options);
            }, this);

            options.emit = method;
            options.arguments = args;

            var success = options.success;
            options.success = function onSuccess(resp) {
                if (success) success(model, resp, options);
                if (options.wait) triggerEmit();
            };
            var error = options.error;
            options.error = function onError(resp) {
                if (error) error(model, resp, options);
                model.trigger('error', model, resp, options);
            };

            var xhr = this.sync('emit', this, options);
            if (!options.wait) triggerEmit();
            return xhr;
        },

        sync: sync
    });

    Streaming.Collection = Backbone.Collection.extend({
        stream: function stream(options) {
            options = options || {};
            if (_.isUndefined(this.cid)) {
                this.cid = _.uniqueId('coll');
            }

            _.defer(_.bind(function () {
                Streaming.add(this, options);
            }, this));
        },

        unstream: function unstream() {
            Streaming.remove(this);
        },

        emit: function emit(method, args, options) {
            if (_.isUndefined(options)) {
                options = args ? _.clone(args) : {};
                args = undefined;
            } else {
                options = options ? _.clone(options) : {};
            }

            var model = this;

            var triggerEmit = _.bind(function emit() {
                model.trigger('emit emit:' + method, model, options);
            }, this);

            options.emit = method;
            options.arguments = args;

            var success = options.success;
            options.success = function onSuccess(resp) {
                if (success) success(model, resp, options);
                if (options.wait) triggerEmit();
            };
            var error = options.error;
            options.error = function onError(resp) {
                if (error) error(model, resp, options);
                model.trigger('error', model, resp, options);
            };

            var xhr = this.sync('emit', this, options);
            if (!options.wait) triggerEmit();
            return xhr;
        },

        sync: sync
    });

    root.Streaming = Streaming;
})(this);

