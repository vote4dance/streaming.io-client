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

    setup: function setup(socket) {
        streaming.socket = socket;

        socket.on('connect', function() {
            streaming.connect(socket);
        });

        socket.on('disconnect', function () {
            streaming.disconnect();
        });

        socket.on('stream', streaming.sync);
    },

    sync: function sync(message) {
        var item = streaming.subscriptions[message.url];
        item.cache = message.data;
        streaming.apply(message.data, item.clients);
    },

    apply: function apply(data, items) {
        _.each(items, function (model) {
            if (!model.streaming) {
                return;
            }

            var initial = model.streaming.initial;
            model.streaming.initial = false;

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

        streaming.send("stream", { url: url }, function (err, response) {
            delete item.updating;

            if (!err) {
                item.cache = response;
                streaming.apply(response, item.clients);
            }
        });
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

        var item = streaming.subscriptions[url];
        var initial = !_.isUndefined(options.initial) ? options.initial : true;
        if (!_.isUndefined(item)) {
            if (item.timeout) {
                clearTimeout(item.timeout);
                delete item.timeout;
            }

            target.streaming = { url: url, initial: initial };
            item.clients[target.cid] = target;

            if (item.cache) {
                streaming.apply(item.cache, [target]);
            }
            return;
        } else {
            item = { clients: {} };
            item.clients[target.cid] = target;

            target.streaming = { url: url, initial: initial };
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
            callback(err, response);
        });
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

    emit: function(method, arguments, options) {
        if (_.isUndefined(options)) {
            options = arguments;
            arguments = undefined;
        }

        options = options ? _.clone(options) : {};
        var model = this;

        var triggerEmit = function() {
            model.trigger('emit emit:' + method, model, model.collection, options);
        };

        options.emit = method;
        options.arguments = arguments;

        var success = options.success;
        options.success = function(resp) {
            if (options.wait) triggerEmit();
            if (success) {
                success(model, resp);
            }
        };

        var xhr = (this.sync || Backbone.sync).call(this, 'emit', this, options);
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

    emit: function () {
        alert("Emitting is not yet supported on collections");
    }
});
