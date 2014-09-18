this.streaming = {};

Backbone.sync = function (method, model, options) {
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

_.extend(streaming, {
    subscriptions: {},
    callbacks: {},

    setUser: function (user_id) {
        this.user_id = user_id;
    },

    setup: function setup(socket) {
        streaming.socket = socket;

        var webSql = false;
        if (/safari/i.test(navigator.userAgent)) {
            var version = /version\/([0-9]+)\.([0-9]+)/i.exec(navigator.userAgent);
            if (version && version[1] == 8) {
                webSql = true;
            }
        }



        this.user_id = 0;
        if (webSql) {
            this.db = new PouchDB('streaming.io', {adapter: 'websql'});
            if (!this.db) {
                this.db = new PouchDB('streaming.io', {adapter: 'idb'});
            }
        } else {
            this.db = new PouchDB('streaming.io');
        }
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

        socket.on('connect', function() {
            streaming.connect(socket);
        });

        socket.on('disconnect', function () {
            streaming.disconnect();
        });

        socket.on('stream', _.bind(streaming.sync, streaming));
    },

    sync: function sync(message) {
        var item = streaming.subscriptions[message.url];
        var data = streaming.uncompress(message.data);
        item.cache = data;
        streaming.apply(data, item.clients);

        this.db.put({
            _id: message.url,
            _rev: item.revision,
            hash: message.hash,
            data: message.data,
            user_id: this.user_id,
            expire: new Date(new Date().getTime() + 2 * 60 * 60 * 1000)
        });
    },

    apply: function apply(data, items) {
        _.each(items, function (model) {
            if (!model.streaming) {
                return;
            }

            var initial = model.streaming.options.initial;
            model.streaming.options.initial = false;

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
                    model.reset(model.parse(data));
                }

            } else {
                model.reset(model.parse(data));
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
                message.hash = doc.hash;
                item.revision = doc._rev;

                if ((doc.user_id == this.user_id) && item.options.precache) {
                    item.cache = streaming.uncompress(doc.data);
                    streaming.apply(item.cache, item.clients);
                }
            }

            streaming.send("stream", message, _.bind(function (err, response) {
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
                        });
                    }

                    if (!item.cache) {
                        item.cache = data = streaming.uncompress(doc.data);
                    }
                } else {
                    item.cache = data = streaming.uncompress(response.data);
                    this.db.put({
                        _id: url,
                        _rev: item.revision,
                        hash: response.hash,
                        data: response.data,
                        user_id: this.user_id,
                        expire: new Date(new Date().getTime() + 2 * 60 * 60 * 1000)
                    });
                }

                if (data) {
                    streaming.apply(data, item.clients);
                }
            }, this));

        }, this));
    },

    connect: function (socket) {
        streaming.socket = socket;

        _.each(streaming.subscriptions, function (item, url) {
            streaming.emit(url, item);
        });
    },

    disconnect: function () {
        _.each(streaming.subscriptions, function (item) {
            delete item.cache;
            if (item.timeout) {
                clearTimeout(item.timeout);
                delete item.timeout;
            }
        });

        _.each(streaming.callbacks, function (callback, id) {
            callback("Disconnected");
        });
        streaming.callbacks = {};

        delete streaming.socket;
    },

    add: function (target, options) {
        var url = target.url();
        if (target.streaming) {
            return;
        }

        options = options ? _.clone(options) : {};
        _.defaults(options, {
            "precache": target.hasOwnProperty("precache") && !!target.precache,
            "initial": true
        });

        var item = streaming.subscriptions[url];
        if (!_.isUndefined(item)) {
            if (item.timeout) {
                clearTimeout(item.timeout);
                delete item.timeout;
            }

            target.streaming = { url: url, options: options };
            item.clients[target.cid] = target;

            if (item.cache) {
                streaming.apply(item.cache, [target]);
            }
            return;
        } else {
            item = { clients: {}, options: options };
            item.clients[target.cid] = target;

            target.streaming = { url: url, options: options };
            streaming.subscriptions[url] = item;
        }

        if (_.isUndefined(streaming.socket)) {
            return;
        }

        streaming.emit(url, item);
    },

    remove: function (target) {
        if (target.streaming) {
            var url = target.streaming.url;
            var item = streaming.subscriptions[url];

            delete item.clients[target.cid];
            if (_.keys(item.clients).length == 0) {
                item.timeout = setTimeout(function () {
                    streaming.socket.emit("unstream", { url: url }, function (err) {});
                    delete streaming.subscriptions[url];
                }, 5000);
            }

            delete target.streaming;
        }
    },

    send: function (type, message, callback) {
        if (!streaming.socket) {
            _.defer(function () {
                callback("No connection");
            });
            return;
        }

        var id = _.uniqueId('cb');
        streaming.callbacks[id] = callback;

        streaming.socket.emit(type, message, function (err, response) {
            delete streaming.callbacks[id];
            callback(err, streaming.uncompress(response));
        });
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

_.extend(Backbone.Model.prototype, {
    stream: function () {
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
    }
});

_.extend(Backbone.Collection.prototype, {
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
    }
});
