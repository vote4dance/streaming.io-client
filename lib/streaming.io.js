(function () {
    var streaming = {};

    _.extend(streaming, {
        subscriptions: {},
        callbacks: {},

        setUser: function (user_id) {
            this.user_id = user_id;
        },

        setup: function setup(socket) {
            this.socket = socket;

            this.user_id = 0;
            this.db = new PouchDB('streaming.io', {auto_compaction: true});
            this.db.allDocs({include_docs: true}, _.bind(function (err, response) {
                var deletes = [];

                if (!err && response) {
                    var now = Date.now();
                    _.each(response.rows, function (row) {
                        if (row.doc.expire && row.doc.expire > now) {
                            return;
                        }
                        row.doc._deleted = true;
                        deletes.push(row.doc);
                    });
                }

                this.db.bulkDocs({
                    docs: deletes
                }, _.bind(function () {
                    this.db.compact();
                }, this));
            }, this));

            socket.on('connect', _.bind(function() {
                this.connect(socket);
            }, this));

            socket.on('disconnect', _.bind(function () {
                this.disconnect();
            }, this));

            socket.on('stream', _.bind(function (message) {
                this.sync(message);
            }, this));
        },

        sync: function sync(message) {
            var item = this.subscriptions[message.url];
            if (!item) {
                return;
            }

            this.db.put({
                _id: message.url,
                _rev: item.revision,
                hash: message.hash,
                data: message.data,
                user_id: this.user_id,
                expire: new Date(new Date().getTime() + 2 * 60 * 60 * 1000)
            }).then(function (response) {
                item.revision = response.rev;
            });

            var data = item.cache = this.uncompress(message.data);
            try {
                this.apply(data, item.clients);
            } catch (e) {
                console.error(e.stack ? e.stack : e.message);
            }
        },

        apply: function apply(data, items) {
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
        },

        emit: function emit(url, item) {
            item.updating = true;

            var message = {
                url: url
            };

            this.db.get(url, _.bind(function (err, doc) {
                if (!err && doc) {
                    item.revision = doc._rev;

                    if (doc.data && (doc.user_id == this.user_id) && item.options.precache) {
                        message.hash = doc.hash;
                        item.cache = this.uncompress(doc.data);

                        try {
                            this.apply(item.cache, item.clients);
                        } catch (e) {
                            console.error(e.stack ? e.stack : e.message);
                        }
                    }
                }

                this.send("stream", message, _.bind(function (err, response) {
                    delete item.updating;

                    if (err) {
                        return;
                    }

                    var data;
                    if (response.hash == message.hash) {
                        var limit = new Date().getTime() + 1 * 60 * 60 * 1000;
                        if (doc.expire < limit || (doc.user_id != this.user_id)) {
                            this.db.put({
                                _id: url,
                                _rev: item.revision,
                                hash: message.hash,
                                data: doc.data,
                                user_id: this.user_id,
                                expire: new Date(new Date().getTime() + 2 * 60 * 60 * 1000)
                            }).then(function (response) {
                                item.revision = response.rev;
                            });
                        }

                        if (!item.cache) {
                            item.cache = data = this.uncompress(doc.data);
                        }
                    } else {
                        item.cache = data = this.uncompress(response.data);
                        this.db.put({
                            _id: url,
                            _rev: item.revision,
                            hash: response.hash,
                            data: response.data,
                            user_id: this.user_id,
                            expire: new Date(new Date().getTime() + 2 * 60 * 60 * 1000)
                        }).then(function (response) {
                            item.revision = response.rev;
                        });
                    }

                    if (data) {
                        try {
                            this.apply(data, item.clients);
                        } catch (e) {
                            console.error(e.stack ? e.stack : e.message);
                        }
                    }
                }, this));

            }, this));
        },

        connect: function (socket) {
            this.socket = socket;

            _.each(this.subscriptions, function (item, url) {
                this.emit(url, item);
            }, this);
        },

        disconnect: function () {
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
        },

        add: function (target, options) {
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
        },

        remove: function (target) {
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
        },

        send: function (type, message, callback) {
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
        },

        uncompress: function (data) {
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
        }
    });

    var sync = function (method, model, options) {
        var message = {
            method: method,
            url: model.url(),
            cid: model.cid,
            data: options.arguments || model.toJSON() || {},
            emit: options.emit
        };

        streaming.send("sync", message, function (err, response) {
            if (err) {
                options.error(err);
            } else {
                options.success(response);
            }
        });
    };

    streaming.Model = Backbone.Model.extend({
        stream: function () {
            console.trace();
            alert("Streaming models is not currently supported");
        },

        unstream: function () {},

        incrementVersion: function () {
            var versionName = this.versionAttribute;
            if (!_.isUndefined(versionName)) {
                this.set(versionName, this.parseVersionNumber(this.get(versionName)) + 1, {silent: true});
            }
        },

        parseVersionNumber: function (versionNumber) {
            return _.isFinite(versionNumber) ? versionNumber : 0;
        },

        parse: function (resp) {
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

        emit: function(method, args, options) {
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
            options.success = function (resp) {
                if (success) success(model, resp, options);
                if (options.wait) triggerEmit();
            };
            var error = options.error;
            options.error = function (resp) {
                if (error) error(model, resp, options);
                model.trigger('error', model, resp, options);
            }

            var xhr = this.sync('emit', this, options);
            if (!options.wait) triggerEmit();
            return xhr;
        },

        sync: sync
    });

    streaming.Collection = Backbone.Collection.extend({
        stream: function (options) {
            options = options || {};
            if (_.isUndefined(this.cid)) {
                this.cid = _.uniqueId('coll');
            }

            _.defer(_.bind(function () {
                streaming.add(this, options);
            }, this));
        },

        unstream: function () {
            streaming.remove(this);
        },

        emit: function (method, args, options) {
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
            options.success = function (resp) {
                if (success) success(model, resp, options);
                if (options.wait) triggerEmit();
            };
            var error = options.error;
            options.error = function (resp) {
                if (error) error(model, resp, options);
                model.trigger('error', model, resp, options);
            }

            var xhr = this.sync('emit', this, options);
            if (!options.wait) triggerEmit();
            return xhr;
        },

        sync: sync
    });

    window.Streaming = streaming;
})();

