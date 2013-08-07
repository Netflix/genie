(function (window) {

    var pagerJsModule = function ($, ko) {

        "use strict";

        /**
         * @module pager
         * @readme README.md
         */

        /**
         *
         * @param fn
         * @param scope
         * @return {Function}
         */
        var makeComputed = function (fn, scope) {
            return function () {
                var args = arguments;
                return ko.computed(function () {
                    return fn.apply(scope, args);
                });
            };
        };


        var pager = {};

        pager.page = null;

        pager.now = function () {
            if (!Date.now) {
                return (new Date()).valueOf();
            } else {
                return Date.now();
            }
        };


        /**
         * @method extendWithPage
         * @static
         * @param {Observable} viewModel
         */
        pager.extendWithPage = function (viewModel) {
            var page = new pager.Page();
            viewModel['$__page__'] = page;
            pager.page = page;

            // initialize computed observables that depend on pager.page
            pager.activePage$ = makeComputed(pager.getActivePage, pager)();
        };

        var fire = function (scope, name, options) {
            options = options || {};
            options.page = scope;
            // first fire the global method
            pager[name].fire(options);
            // then call the local callback
            if (scope.val(name)) {
                scope.val(name)(options);
            }
        };

        // Add 9 callbacks on pager
        $.each(['onBindingError', 'onSourceError', 'onNoMatch', 'onMatch',
            'beforeRemove', 'afterRemove',
            'beforeHide', 'afterHide',
            'beforeShow', 'afterShow'], function (i, n) {
            pager[n] = $.Callbacks();
        });

        /**
         *
         * @param {String[]} route
         */
        pager.showChild = function (route) {
            // trim empty array
            var trimmedRoute = (route && route.length === 1 && route[0] === '') ? [] : route;
            pager.page.showPage(trimmedRoute);
        };

        pager.getParentPage = function (bindingContext) {
            // search this context/$data until either root is accessed or no page is found
            while (bindingContext) {
                // get first parent page, but exclude pages with urlToggle: none
                if (bindingContext.$page && bindingContext.$page.val('urlToggle') !== 'none') {
                    return bindingContext.$page;
                } else if (bindingContext.$data && bindingContext.$data.$__page__) {
                    return bindingContext.$data.$__page__;
                }
                bindingContext = bindingContext.$parentContext;
            }
            return null;
        };

        // set this to a random value in order to verify that the navigation should happen
        // Is cleaned after every goTo.
        var goToKey = null;

        var currentAsyncDeferred = null;

        /**
         *
         * Takes a complete, working, path as parameter. *Not* a route, relative route or Page-object.
         *
         * @param {String} path
         */
        var goTo = function (path) {
            // reject any async navigation in progress
            if (currentAsyncDeferred) {
                currentAsyncDeferred.reject({cancel: true});
            }
            goToKey = null;
            // strip # (or #!/)
            if (path.substring(0, pager.Href.hash.length) === pager.Href.hash) {
                path = path.slice(pager.Href.hash.length);
            }
            // split on '/' and decode
            var hashRoute = parseHash(path);
            // trigger navigation
            pager.showChild(hashRoute);
        };
        pager.goTo = goTo;

        /**
         *
         * navigate takes a complete, working, path as parameter. *NOT* a route, object or pager.Page-object.
         *
         * @param {String} path
         */
        pager.navigate = function (path) {
            if (pager.useHTML5history) {
                pager.Href5.history.pushState(null, null, path);
            } else {
                location.hash = path;
            }
        };


        var parseHash = function (hash) {
            return $.map(hash.replace(/\+/g, ' ').split('/'), decodeURIComponent);
        };


// common KnockoutJS helpers
        var _ko = {};

        _ko.value = ko.utils.unwrapObservable;

        _ko.arrayValue = function (arr) {
            return $.map(arr, function (e) {
                return _ko.value(e);
            });
        };

        var parseStringAsParameters = function (query) {
            var match,
                urlParams = {},
                search = /([^&=]+)=?([^&]*)/g;

            while (match = search.exec(query)) {
                urlParams[match[1]] = match[2];
            }
            return urlParams;
        };

        var splitRoutePartIntoNameAndParameters = function (routePart) {
            if (!routePart) {
                return {name: null, params: {}};
            }
            var routeSplit = routePart.split('?');
            var name = routeSplit[0];
            var paramsString = routeSplit[1];
            var params = {};
            if (paramsString) {
                params = parseStringAsParameters(paramsString);
            }
            return {
                name: name,
                params: params
            };
        };

        /**
         * @class pager.ChildManager
         *
         * @param {pager.Page[]} children
         * @param {pager.Page} page
         */
        pager.ChildManager = function (children, page) {

            this.currentChildO = ko.observable(null);
            var me = this;
            this.page = page;
            // Used by showChild to find out if the navigation is still current.
            // In needed since the navigation is asynchronous and another navigation might happen in between.
            this.timeStamp = pager.now();

            /**
             * @method pager.ChildManager#hideChild
             */
            this.hideChild = function () {
                var currentChild = me.currentChildO();
                if (currentChild) {
                    if (!currentChild.isStartPage()) {
                        currentChild.hidePage(function () {
                        });
                        me.currentChildO(null);
                    }
                }
            };

            /**
             * Show the sub-page in this ChildManager's page that matches the route.
             *
             * @method pager.ChildManager#showChild
             * @param {String[]} route route to match a sub-page to. Can be on the form `['foo','bar?x=22&y=11']`.
             */
            this.showChild = function (route) {
                var showOnlyStart = route.length === 0;
                this.timeStamp = pager.now();
                var timeStamp = this.timeStamp;
                var oldCurrentChild = me.currentChildO();
                var currentChild = null;
                var match = false;
                var currentRoutePair = splitRoutePartIntoNameAndParameters(route[0]);
                var currentRoute = currentRoutePair.name;
                var wildcard = null;
                $.each(children(), function (childIndex, child) {
                    if (!match) {
                        var id = child.getId();
                        if (id === currentRoute ||
                            ((currentRoute === '' || currentRoute == null) && child.isStartPage())) {
                            match = true;
                            currentChild = child;
                        }
                        if (id === '?') {
                            wildcard = child;
                        }
                    }
                });
                // find modals in parent - but only if 1) no match is found, 2) this page got a parent and 3) this page is not a modal!
                var isModal = false;

                var currentChildManager = me;

                var findMatchModalOrWildCard = function (childIndex, child) {
                    if (!match) {
                        var id = child.getId();
                        var modal = child.getValue().modal;
                        if (modal) {
                            if (id === currentRoute ||
                                ((currentRoute === '' || currentRoute == null) && child.isStartPage())) {
                                match = true;
                                currentChild = child;
                                isModal = true;
                            }
                            if (id === '?' && !wildcard) {
                                wildcard = child;
                                isModal = true;
                            }
                        }
                    }
                };

                while (!currentChild && currentChildManager.page.parentPage && !currentChildManager.page.getValue().modal) {
                    var parentChildren = currentChildManager.page.parentPage.children;
                    $.each(parentChildren(), findMatchModalOrWildCard);
                    if (!currentChild) {
                        currentChildManager = currentChildManager.page.parentPage.childManager;
                    }
                }

                if (!currentChild && wildcard && !showOnlyStart) {
                    currentChild = wildcard;
                    //me.currentChild.currentId = currentRoute;
                }

                me.currentChildO(currentChild);
                if (currentChild) {

                    if (isModal) {
                        currentChild.currentParentPage(me.page);
                    } else {
                        currentChild.currentParentPage(null);
                    }

                }

                var onFailed = function () {
                    fire(me.page, 'onNoMatch', {route: route});
                };

                var showCurrentChild = function () {
                    fire(me.page, 'onMatch', {route: route});
                    var guard = _ko.value(currentChild.getValue().guard);
                    if (guard) {
                        guard(currentChild, route, function () {
                            if (me.timeStamp === timeStamp) {
                                currentChild.showPage(route.slice(1), currentRoutePair, route[0]);
                            }
                        }, oldCurrentChild);
                    } else {
                        currentChild.showPage(route.slice(1), currentRoutePair, route[0]);
                    }
                };

                if (oldCurrentChild && oldCurrentChild === currentChild) {
                    showCurrentChild();
                } else if (oldCurrentChild) {
                    oldCurrentChild.hidePage(function () {
                        if (currentChild) {
                            showCurrentChild();
                        } else {
                            onFailed();
                        }
                    });
                } else if (currentChild) {
                    showCurrentChild();
                } else {
                    onFailed();
                }
            };
        };

        /**
         *
         * @class pager.Page
         *
         * @param {Node} element
         * @param {Object} valueAccessor
         * @param {String} valueAccessor.id
         * @param {Observable} valueAccessor.with
         * @param {Function} valueAccessor.withOnShow
         * @param {String/Function} valueAccessor.source
         * @param {String/Function} valueAccessor.sourceLoaded
         * @param {Number/Boolean} valueAccessor.sourceCache
         * @param {String} valueAccessor.frame
         * @param {Boolean} valueAccessor.modal
         * @param {Boolean} valueAccessor.deep
         * @param {Function} valueAccessor.beforeHide
         * @param {Function} valueAccessor.beforeShow
         * @param {Function} valueAccessor.afterHide
         * @param {Function} valueAccessor.hideElement
         * @param {Function} valueAccessor.showElement
         * @param {Function} valueAccessor.loader
         * @param {Function} valueAccessor.onNoMatch
         * @param {Function} valueAccessor.guard
         * @param {Object} valueAccessor.params
         * @param {Object} valueAccessor.vars
         * @param {String} valueAccessor.fx
         * @param {String} valueAccessor.urlToggle can be either null (default), "none" or "show"
         * @param allBindingsAccessor
         * @param {Observable} viewModel
         * @param bindingContext
         */
        pager.Page = function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
            /**
             *
             * @type {Node}
             */
            this.element = element;
            /**
             *
             * @type {Observable}
             */
            this.valueAccessor = valueAccessor;
            /**
             *
             * @type {*}
             */
            this.allBindingsAccessor = allBindingsAccessor;
            /**
             *
             * @type {Observable}
             */
            this.viewModel = viewModel;
            /**
             *
             * @type {*}
             */
            this.bindingContext = bindingContext;

            /**
             *
             * @type {ObservableArray}
             */
            this.children = ko.observableArray([]);

            /**
             *
             * @type {pager.ChildManager}
             */
            this.childManager = new pager.ChildManager(this.children, this);
            /**
             *
             * @type {pager.Page}
             */
            this.parentPage = null;
            /**
             *
             * @type {String}
             */
            this.currentId = null;

            this.getCurrentId = ko.observable();

            /**
             *
             *
             * @type {Observable}
             */
            this.ctx = null;

            /**
             *
             * @type {Observable/pager.Page}
             */
            this.currentParentPage = ko.observable(null);


            /**
             *
             * @type {Observable}
             */
            this.isVisible = ko.observable(false);

            this.originalRoute = ko.observable(null);

            this.route = null;
        };

        var p = pager.Page.prototype;

        /**
         * @method pager.Page#val
         *
         * @param {String} key
         * @return {Object} an un-boxed configuration property
         */
        p.val = function (key) {
            return _ko.value(this.getValue()[key]);
        };

        /**
         * @method pager.Page#currentChildPage
         *
         * Returns an observable to the child page.
         *
         * @returns {Observable}
         */
        p.currentChildPage = function () {
            return this.childManager.currentChildO;
        };

        /**
         *
         * @param {String} key relative (to this page) or absolute page path
         * @return {Observable} page
         */
        p.find = function (key) {
            var k = _ko.value(key);
            var currentRoot = this;
            if (k.substring(0, 1) === '/') {
                currentRoot = pager.page;
                k = k.slice(1);
            } else {
                while (k.substring(0, 3) === '../') {
                    currentRoot = (currentRoot.currentParentPage && currentRoot.currentParentPage()) ?
                        currentRoot.currentParentPage() :
                        currentRoot.parentPage;
                    k = k.slice(3);
                }
            }
            var route = parseHash(k);
            $.each(route, function (_, r) {
                currentRoot = currentRoot.child(r)();
            });
            return currentRoot;
        };

        p.find$ = function (key) {
            return makeComputed(this.find, this)(key);
        };

        var absolutePathToRealPath = function (path) {
            if (pager.useHTML5history) {
                return $('base').attr('href') + path;
            } else {
                return pager.Href.hash + path;
            }
        };

        /**
         *
         * Utility method to generate a complete (computed observable) path relative to the current Page.
         *
         * @param {String/pager.Page/Object} path can either be relative path, a Page-object or a {path: params:}-object.
         * @return {String}
         */
        p.path = function (path) {
            var me = this;
            var p = _ko.value(path);
            if (p && typeof(p) === 'object' && p.path && p.params && !(p instanceof pager.Page)) {
                var objectPath = p.path;
                var params = p.params;
                return me.path(objectPath) + '?' + $.param(params);
            } else {
                var page;
                if (p == null || p === '') {
                    page = me;
                } else if (p instanceof pager.Page) {
                    page = p;
                } else { // if string
                    if (p.substring(0, 1) === '/') {
                        var pagePath = pager.page.getFullRoute()().join('/') + p.substring(1);
                        return absolutePathToRealPath(pagePath);
                    }
                    var parentsToTrim = 0;
                    while (p.substring(0, 3) === '../') {
                        parentsToTrim++;
                        p = p.slice(3);
                    }

                    var fullRoute = me.getFullRoute()();
                    var parentPath = fullRoute.slice(0, fullRoute.length - parentsToTrim).join('/');
                    var fullPathWithoutHash = (parentPath === '' ? '' : parentPath + '/') + p;
                    return absolutePathToRealPath(fullPathWithoutHash);
                }
                return absolutePathToRealPath(page.getFullRoute()().join('/'));
            }
        };

        p.path$ = function (path) {
            return makeComputed(this.path, this)(path);
        };

        /**
         *
         * @param {Function} fn should return a $.Deferred (NOT a promise since async should be able to reject it).
         * @param {String/Object} ok route (e.g. '/some/path' or '../some/path'). Should not contain '#!/'.
         * @param {String/Object} notOk route (e.g. '/some/path' or '../some/path'). Should not contain '#!/'.
         * @param {Observable} [state]
         * @return {Function}
         */
        p.async = function (fn, ok, notOk, state) {
            var me = this;
            return function () {
                if (currentAsyncDeferred) {
                    currentAsyncDeferred.reject({cancel: true});
                }
                var result = fn();
                currentAsyncDeferred = result;
                if (state) {
                    state(result.state());
                }
                var key = Math.random();
                goToKey = key;

                result.done(function () {
                    if (state) {
                        state(result.state());
                    }
                    if (key === goToKey) {
                        pager.navigate(me.path(ok));
                    }
                });
                result.fail(function (data) {
                    if (state) {
                        state(result.state());
                    }
                    var cancel = data && data.cancel;
                    if (key === goToKey) {
                        if (!cancel && notOk) {
                            pager.navigate(me.path(notOk));
                        }
                    }
                });
            };
        };

        /**
         * @method pager.Page#showPage
         *
         * @param route
         * @param [pageRoute]
         * @param [originalRoute]
         */
        p.showPage = function (route, pageRoute, originalRoute) {
            var m = this,
                currentId = m.currentId,
                params = m.pageRoute ? m.pageRoute.params : null,
                isVisible = m.isVisible();
            m.currentId = pageRoute ? (pageRoute.name || '') : '';
            m.getCurrentId(m.currentId);
            m.isVisible(true);
            if (originalRoute) {
                m.originalRoute(originalRoute);
            }
            m.route = route;
            m.pageRoute = pageRoute;
            // show if not already visible
            if (!isVisible) {
                m.setParams();
                m.show();
            } else {
                // show if wildcard got new ID
                if (m.getId() === '?' && currentId !== m.currentId) {
                    m.show();
                }
                // update params if they are updated
                if (pageRoute && params !== pageRoute.params) {
                    m.setParams();
                }
            }
            m.childManager.showChild(route);
        };

        /**
         * @method pager.Page#setParams
         *
         */
        p.setParams = function () {
            if (this.pageRoute && this.pageRoute.params) {
                var params = this.pageRoute.params;

                // get view model
                var vm = this.ctx;
                var userParams = this.val('params') || {};
                // for each param for URL
                if ($.isArray(userParams)) {
                    $.each(userParams, function (index, key) {
                        var value = params[key];
                        if (vm[key]) { // set observable ...
                            vm[key](value);
                        } else { // ... or create observable
                            vm[key] = ko.observable(value);
                        }
                    });
                } else {
                    $.each(userParams, function (key, defaultValue) {
                        var value = params[key];
                        var runtimeValue;
                        if (value == null) {
                            runtimeValue = _ko.value(defaultValue);
                        } else {
                            runtimeValue = value;
                        }
                        if (vm[key]) {
                            vm[key](runtimeValue);
                        } else {
                            vm[key] = ko.observable(runtimeValue);
                        }
                    });
                }
            }
            if (this.pageRoute) {
                var nameParam = this.getValue()['nameParam'];
                if (nameParam) {
                    if (typeof nameParam === 'string') {
                        if (this.ctx[nameParam]) { // set observable ...
                            this.ctx[nameParam](this.currentId);
                        } else { // ... or create observable
                            this.ctx[nameParam] = ko.observable(this.currentId);
                        }
                    } else { // is Observable
                        nameParam(this.currentId);
                    }
                }
            }
        };

        /**
         * @method pager.Page#hidePage
         *
         * @param {Function} callback
         */
        p.hidePage = function (callback) {
            var m = this;
            if ('show' !== m.val('urlToggle')) {
                m.hideElementWrapper(callback);
                m.childManager.hideChild();
            } else {
                if (callback) {
                    callback();
                }
            }
        };

        var applyBindingsToDescendants = function (page) {
            try {
                ko.applyBindingsToDescendants(page.childBindingContext, page.element);
            } catch (e) {
                fire(page, 'onBindingError', {error: e});
            }
        };

        /**
         * @method pager.Page#init
         *
         * @return {Object}
         */
        p.init = function () {
            var m = this;
            var urlToggle = m.val('urlToggle');

            var id = m.val('id');
            if (id !== '?') {
                m.getCurrentId(id);
            }

            var existingPage = ko.utils.domData.get(m.element, '__ko_pagerjsBindingData');
            if (existingPage) {
                return { controlsDescendantBindings: true};
            } else {
                ko.utils.domData.set(m.element, '__ko_pagerjsBindingData', m);
            }

            // listen to when the element is removed
            ko.utils.domNodeDisposal.addDisposeCallback(m.element, function () {
                // then remove this Page-instance
                fire(m, 'beforeRemove');
                if (m.parentPage) {
                    m.parentPage.children.remove(m);
                }
                fire(m, 'afterRemove');
            });

            var value = m.getValue();
            if (urlToggle !== 'none') {
                m.parentPage = m.getParentPage();
                m.parentPage.children.push(this);
                m.hideElement();
            }


            // Fetch source
            if (m.val('source')) {
                m.loadSource(m.val('source'));
            }

            m.ctx = null;
            if (value.withOnShow) {
                m.ctx = {};
                m.childBindingContext = m.bindingContext.createChildContext(m.ctx);
                ko.utils.extend(m.childBindingContext, { $page: this });
            } else {
                var context = value['with'] || m.bindingContext['$observableData'] || m.viewModel;
                m.ctx = _ko.value(context);
                m.augmentContext();

                if (ko.isObservable(context)) {
                    var dataInContext = ko.observable(m.ctx);
                    m.childBindingContext = m.bindingContext.createChildContext(dataInContext);
                    ko.utils.extend(m.childBindingContext, {
                        $page: this,
                        $observableData: context
                    });
                    applyBindingsToDescendants(m);
                    context.subscribe(function () {
                        dataInContext(_ko.value(context));
                    });
                } else {
                    m.childBindingContext = m.bindingContext.createChildContext(m.ctx);
                    ko.utils.extend(m.childBindingContext, {
                        $page: this,
                        $observableData: undefined
                    });
                    applyBindingsToDescendants(m);
                }
            }

            if (urlToggle !== 'none') {
                // check if this page should trigger showChild at parent
                var parent = m.parentPage;
                if (parent.route && (parent.route[0] === m.getId() || (parent.route.length && m.getId() === '?') )) {
                    // call once the current event loop is finished.
                    setTimeout(function () {
                        parent.showPage(parent.route);
                    }, 0);
                }
            } else { // urlToggle === 'none'
                // when the page is rendered
                var display = function () {
                    // if the page is visible
                    if ($(m.element).is(':visible')) {
                        // trigger showPage with empty route-array
                        m.showPage([]);
                    }
                };
                setTimeout(display, 0);
                m.getParentPage().isVisible.subscribe(function (x) {
                    if (x) {
                        setTimeout(display, 0);
                    }
                });
            }
            // Bind the page to the config property `bind` if it exists
            var bind = m.getValue()['bind'];
            if (ko.isObservable(bind)) {
                bind(m);
            }

            return { controlsDescendantBindings: true };
        };

        p.augmentContext = function () {
            var m = this;
            var params = m.val('params');
            if (params) {
                if ($.isArray(params)) {
                    $.each(params, function (index, param) {
                        if (!m.ctx[param]) {
                            m.ctx[param] = ko.observable();
                        }
                    });
                } else { // is object
                    $.each(params, function (key, value) {
                        if (!m.ctx[key]) {
                            if (ko.isObservable(value)) {
                                m.ctx[key] = value;
                            } else if (value === null) {
                                params[key] = ko.observable(null);
                                m.ctx[key] = ko.observable(null);
                            } else {
                                m.ctx[key] = ko.observable(value);
                            }
                        }
                    });
                }
            }
            if (this.val('vars')) {
                $.each(this.val('vars'), function (key, value) {
                    if (ko.isObservable(value)) {
                        m.ctx[key] = value;
                    } else {
                        m.ctx[key] = ko.observable(value);
                    }
                });
            }
            var nameParam = this.getValue()['nameParam'];
            if (nameParam && typeof nameParam === 'string') {
                m.ctx[nameParam] = ko.observable(null);
            }
            this.setParams();
        };

        /**
         * @method pager.Page#getValue
         * @returns {Object} value
         */
        p.getValue = function () {
            if (this.valueAccessor) {
                return _ko.value(this.valueAccessor());
            } else {
                return {};
            }
        };

        /**
         * @method pager.Page#getParentPage
         * @return {pager.Page}
         */
        p.getParentPage = function () {
            return pager.getParentPage(this.bindingContext);
        };

        /**
         * @method pager.Page#getId
         * @return String
         */
        p.getId = function () {
            return this.val('id');
        };

        p.id = function () {
            var currentId = this.getCurrentId();
            if (currentId == null || currentId === '') {
                return this.getId();
            } else {
                return currentId;
            }
        };


        /**
         * @method pager.Page#sourceUrl
         *
         * @param {Observable/String} source
         * @return {Observable}
         */
        p.sourceUrl = function (source) {
            var me = this;
            if (this.getId() === '?') {
                return ko.computed(function () {
                    // TODO: maybe make currentId an ko.observable?
                    var path;
                    if (me.val('deep')) {
                        path = [me.currentId].concat(me.route).join('/');
                    } else {
                        path = me.currentId;
                    }
                    return _ko.value(source).replace('{1}', path);
                });
            } else {
                return ko.computed(function () {
                    return _ko.value(source);
                });
            }
        };

        p.loadWithOnShow = function () {
            var me = this;
            if (!me.withOnShowLoaded || me.val('sourceCache') !== true) {
                me.withOnShowLoaded = true;
                me.val('withOnShow')(function (vm) {
                    var childBindingContext = me.bindingContext.createChildContext(vm);
                    me.ctx = vm;
                    // replace the childBindingContext
                    me.childBindingContext = childBindingContext;
                    me.augmentContext();
                    ko.utils.extend(childBindingContext, {$page: me});
                    applyBindingsToDescendants(me);
                    // what is signaling if a page is active or not?
                    if (me.route) {
                        me.childManager.showChild(me.route);
                    }
                }, me);
            }
        };

        /**
         * @method pager.Page#loadSource
         * @param source
         */
        p.loadSource = function (source) {
            var value = this.getValue();
            var me = this;
            var element = this.element;
            var loader = null;
            var loaderMethod = value.loader || pager.loader;
            if (value.frame === 'iframe') {
                var iframe = $('iframe', $(element));
                if (iframe.length === 0) {
                    iframe = $('<iframe></iframe>');
                    $(element).append(iframe);
                }
                if (loaderMethod) {
                    loader = _ko.value(loaderMethod)(me, iframe);
                    loader.load();
                }
                iframe.one('load', function () {
                    if (loader) {
                        loader.unload();
                    }
                    if (value.sourceLoaded) {
                        value.sourceLoaded(me);
                    }
                });
                // TODO: remove src binding and add this binding
                ko.applyBindingsToNode(iframe[0], {
                    attr: {
                        src: this.sourceUrl(source)
                    }
                });
            } else {
                if (loaderMethod) {
                    loader = _ko.value(loaderMethod)(me, me.element);
                    loader.load();
                }
                // TODO: remove all children and add sourceUrl(source)
                var onLoad = function () {
                    // remove load
                    if (loader) {
                        loader.unload();
                    }
                    // apply bindings
                    // TODO: call abstraction that either applies binding or loads view-model

                    if (!me.val('withOnShow')) {
                        applyBindingsToDescendants(me);
                    } else if (me.val('withOnShow')) {
                        me.loadWithOnShow();
                    }

                    // trigger event
                    if (value.sourceLoaded) {
                        value.sourceLoaded(me);
                    }
                    // possibly continue routing
                    if (me.route) {
                        me.childManager.showChild(me.route);
                    }
                };
                if (typeof _ko.value(source) === 'string') {
                    var s = _ko.value(this.sourceUrl(source));
                    koLoad(element, s, function () {
                        onLoad();
                    }, me);
                } else { // should be a method
                    var childrenToRemove = $(element).children();
                    _ko.value(source)(this, function () {
                        $.each(childrenToRemove, function (i, c) {
                            ko.utils.domNodeDisposal.removeNode(c);
                        });
                        onLoad();
                    });
                }
            }
        };

        var rscript = /<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi;

        // a modified version of jQUery.fn.load, where the element is executing removeNode
        // before adding the new node.
        var koLoad = function (element, url, callback, page) {
            var selector, response,
                self = $(element),
                off = url.indexOf(" ");

            if (off >= 0) {
                selector = url.slice(off, url.length);
                url = url.slice(0, off);
            }

            // Request the remote document
            var loadPromise = jQuery.ajax({
                url: url,
                type: 'GET',
                dataType: "html",
                complete: function (jqXHR, status) {
                    if (callback) {
                        self.each(callback, response || [ jqXHR.responseText, status, jqXHR ]);
                    }
                }
            }).done(function (responseText) {

                    // Save response for use in complete callback
                    response = arguments;

                    // clean up the element
                    $.each(self.children(), function (i, c) {
                        ko.utils.domNodeDisposal.removeNode(c);
                    });

                    // See if a selector was specified
                    self.html(selector ?

                        // Create a dummy div to hold the results
                        jQuery("<div>")

                            // inject the contents of the document in, removing the scripts
                            // to avoid any 'Permission Denied' errors in IE
                            .append(responseText.replace(rscript, ""))

                            // Locate the specified elements
                            .find(selector) :

                        // If not, just inject the full result
                        responseText);

                });

            loadPromise.fail(function () {
                fire(page, 'onSourceError', {url: url, xhrPromise: loadPromise});
            });
            return self;
        };

        /**
         * @method pager.Page#show
         * @param {Function} [callback]
         */
        p.show = function (callback) {
            var element = this.element;
            var me = this;
            //var value = me.getValue();
            me.showElementWrapper(callback);
            if (me.val('title')) {
                window.document.title = me.val('title');
            }
            // Fetch source
            if (me.val('sourceOnShow')) {
                if (!me.val('sourceCache') || !element.__pagerLoaded__ ||
                    (typeof(me.val('sourceCache')) === 'number' && element.__pagerLoaded__ + me.val('sourceCache') * 1000 < pager.now())) {
                    element.__pagerLoaded__ = pager.now();
                    me.loadSource(me.val('sourceOnShow'));
                }
            }
            else if (me.val('withOnShow')) {
                me.loadWithOnShow();
            }
        };

        p.titleOrId = function () {
            return this.val('title') || this.id();
        };

        /**
         * @method pager.Page#showElementWrapper
         * @param {Function} callback
         */
        p.showElementWrapper = function (callback) {
            var me = this;
            fire(me, 'beforeShow');
            me.showElement(callback);
            if (me.val('scrollToTop')) {
                me.element.scrollIntoView();
            }
            fire(me, 'afterShow');
        };

        /**
         * @method pager.Page#showElement
         * @param {Function} callback
         */
        p.showElement = function (callback) {
            if (this.val('showElement')) {
                this.val('showElement')(this, callback);
            } else if (this.val('fx')) {
                pager.fx[this.val('fx')].showElement(this, callback);
            } else if (pager.showElement) {
                pager.showElement(this, callback);
            } else {
                $(this.element).show(callback);
            }
        };

        /**
         *
         * @method pager.Page#hideElementWrapper
         * @param {Function} callback
         */
        p.hideElementWrapper = function (callback) {
            this.isVisible(false);
            fire(this, 'beforeHide');
            this.hideElement(callback);
            fire(this, 'afterHide');
        };

        /**
         * @method pager.Page#hideElement
         * @param {Function} [callback]
         */
        p.hideElement = function (callback) {
            if (this.val('hideElement')) {
                this.val('hideElement')(this, callback);
            } else if (this.val('fx')) {
                pager.fx[this.val('fx')].hideElement(this, callback);
            } else if (pager.hideElement) {
                pager.hideElement(this, callback);
            } else {
                $(this.element).hide();
                if (callback) {
                    callback();
                }
            }
        };


        /**
         *
         * @return {Observable}
         */
        p.getFullRoute = function () {
            // either return an already created computed observable
            if (this._fullRoute) {
                return this._fullRoute;
            } else {
                // or create a computed observable..
                this._fullRoute = ko.computed(function () {
                    var res = null;
                    if (this.currentParentPage && this.currentParentPage()) {
                        res = this.currentParentPage().getFullRoute()().slice(0);
                        res.push((this.originalRoute() || this.getId()));
                        return res;
                    } else if (this.parentPage) {
                        res = this.parentPage.getFullRoute()().slice(0);
                        res.push((this.originalRoute() || this.getId()));
                        return res;
                    } else { // is root page
                        return [];
                    }
                }, this);
                // ... and return it
                return this._fullRoute;
            }
        };

        /**
         * Return the role of the page (either `next` or `start`).
         *
         *     <div data-bind="page: {id: 'x', role: 'start'}"></div>
         *
         * Specifying role `start` gives the page the same behaviour as if the page
         * had `{id: 'start'}`.
         *
         * @return {String}
         */
        p.getRole = function () {
            return this.val('role') || 'next';
        };

        /**
         * @method pager.Page#isStartPage
         *
         * Returns true if id is start or role is start.
         *
         * @returns {boolean}
         */
        p.isStartPage = function () {
            return this.getId() === 'start' || this.getRole() === 'start';
        };

        p.nullObject = new pager.Page();
        p.nullObject.children = ko.observableArray([]);

        /**
         * Get the child page, by name, of the current child as a
         * computed observable.
         *
         *     // get the child page of somePage, with the id admin
         *     var adminObservable = somePage.child('admin');
         *     // run () to get the object, getId is a method on pager.Page
         *     var id = adminObservable().getId();
         *     // id is admin
         *     console.log(id === 'admin');
         *
         * @param {String} key
         * @return {Observable}
         */
        p.child = function (key) {
            var me = this;
            if (me._child == null) {
                me._child = {};
            }
            if (!me._child[key]) {
                me._child[key] = ko.computed(function () {
                    var child = $.grep(this.children(), function (c) {
                        return c.id() === key;
                    })[0];
                    return child || this.nullObject;
                }, this);
            }
            return me._child[key];
        };

        pager.getActivePage = function () {
            var active = pager.page;
            while (active.currentChildPage()() != null) {
                active = active.currentChildPage()();
            }
            return active;
        };

        ko.bindingHandlers.page = {
            init: function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
                var page = null;
                if (_ko.value(valueAccessor()) instanceof pager.Page) {
                    page = _ko.value(valueAccessor());
                    page.element = element;
                    if (page.allBindingsAccessor == null) {
                        page.allBindingsAccessor = allBindingsAccessor;
                    }
                    if (page.viewModel == null) {
                        page.viewModel = viewModel;
                    }
                    if (page.bindingContext == null) {
                        page.bindingContext = bindingContext;
                    }
                } else {
                    page = new pager.Page(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext);
                }
                return page.init();
            }
        };

// page-href

        /**
         *
         * @type {Boolean}
         */
        pager.useHTML5history = false;
        /**
         *
         * @type {String}
         */
        pager.rootURI = '/';

        pager.Href = function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
            this.element = element;
            this.bindingContext = bindingContext;
            this.path = ko.observable();
            this.pageOrRelativePath = ko.observable(valueAccessor);
        };

        var hp = pager.Href.prototype;

        hp.getParentPage = function () {
            return pager.getParentPage(this.bindingContext);
        };

        hp.init = function () {
            var me = this;
            var page = me.getParentPage();

            me.path = ko.computed(function () {
                var value = _ko.value(me.pageOrRelativePath()());
                return page.path(value);
            });
        };

        pager.Href.hash = '#';

        hp.bind = function () {
            ko.applyBindingsToNode(this.element, {
                attr: {
                    'href': this.path
                }
            });
        };

        hp.update = function (valueAccessor) {
            this.pageOrRelativePath(valueAccessor);
        };

        pager.Href5 = function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
            pager.Href.apply(this, arguments);
        };

        pager.Href5.prototype = new pager.Href();

        pager.Href5.history = window.History;

        pager.Href5.prototype.bind = function () {
            var self = this;
            ko.applyBindingsToNode(self.element, {
                attr: {
                    'href': self.path
                },
                click: function () {
                    var path = self.path();
                    if (path === '' || path === '/') {
                        path = $('base').attr('href');
                    }
                    pager.Href5.history.pushState(null, null, path);
                }
            });
        };

        ko.bindingHandlers['page-href'] = {
            init: function (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
                var Cls = pager.useHTML5history ? pager.Href5 : pager.Href;
                var href = new Cls(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext);
                href.init();
                href.bind();
                element.__ko__page = href;
            },
            update: function (element, valueAccessor) {
                element.__ko__page.update(valueAccessor);
            }
        };

        pager.fx = {};

        pager.fx.cssAsync = function (css) {
            return {
                showElement: function (page, callback) {
                    var $e = $(page.element);
                    $e.addClass(css);
                    $e.show();
                    var i = setInterval(function () {
                        clearInterval(i);
                        $e.addClass(css + '-in');
                    }, 10);
                    var i2 = setInterval(function () {
                        clearInterval(i2);
                        if (callback) {
                            callback();
                        }
                    }, 300);
                },
                hideElement: function (page, callback) {
                    var $e = $(page.element);
                    if (!page.pageHiddenOnce) {
                        page.pageHiddenOnce = true;
                        $e.hide();
                    } else {
                        $e.removeClass(css + '-in');
                        var i = setInterval(function () {
                            clearInterval(i);
                            if (callback) {
                                callback();
                            }
                            $e.hide();
                        }, 300);
                    }
                }
            };
        };

        pager.fx.zoom = pager.fx.cssAsync('pagerjs-fx-zoom');
        pager.fx.flip = pager.fx.cssAsync('pagerjs-fx-flip');
        pager.fx.popout = pager.fx.cssAsync('pagerjs-fx-popout-modal');

        pager.fx.jQuerySync = function (show, hide) {
            return {
                showElement: function (page, callback) {
                    show.call($(page.element), 300, callback);
                },
                hideElement: function (page, callback) {
                    hide.call($(page.element), 300, function () {
                        $(page.element).hide();
                    });
                    if (callback) {
                        callback();
                    }
                }
            };
        };

        pager.fx.slide = pager.fx.jQuerySync($.fn.slideDown, $.fn.slideUp);
        pager.fx.fade = pager.fx.jQuerySync($.fn.fadeIn, $.fn.fadeOut);

        /**
         *
         * @param {String/Object} options
         */
        pager.startHistoryJs = function (options) {
            var id = typeof options === 'string' ? options : null;
            if (id) {
                pager.Href5.history.pushState(null, null, id);
            }

            // Bind to StateChange Event
            pager.Href5.history.Adapter.bind(window, 'statechange', function () {
                var relativeUrl = pager.Href5.history.getState().url.replace(pager.Href5.history.getBaseUrl(), '');
                goTo(relativeUrl);
            });
            pager.Href5.history.Adapter.bind(window, 'anchorchange', function () {
                goTo(location.hash);
            });
            if (!options || !options.noGo) {
                goTo(pager.Href5.history.getState().url.replace(pager.Href5.history.getBaseUrl(), ''));
            }
        };

        /**
         * This is the hash-based start-method.
         *
         * You should only use this method if you do not want HTML5 history support and
         * do not want IE6/7 support.
         *
         * @method start
         * @param {String/Object} options
         * @static
         */
        pager.start = function (options) {
            var id = typeof options === 'string' ? options : null;
            if (id) {
                window.location.hash = pager.Href.hash + id;
            }
            var onHashChange = function () {
                goTo(window.location.hash);
            };
            $(window).bind('hashchange', onHashChange);

            if (!options || !options.noGo) {
                onHashChange();
            }
        };
        return pager;
    };


    /*--------------------------------------------------------------------------*/

    var define = window.define;

    // expose Pager.js
    // This code is a modified version of the AMD-fallback code found in Lo-Dash
    // (https://raw.github.com/bestiejs/lodash/master/lodash.js)
    // some AMD build optimizers, like r.js, check for specific condition patterns like the following:
    if (typeof define === 'function' && typeof define.amd === 'object' && define.amd) {
        // define as an anonymous module so, through path mapping, it can be
        // referenced as any module
        define('pager', ['knockout', 'jquery'], function (ko) {
            return pagerJsModule($, ko);
        });
    } else {
        // without AMD
        window.pager = pagerJsModule($, ko);
    }


}(window));
