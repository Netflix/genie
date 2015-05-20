<!--

Copyright 2015 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-->
<%@ page language="java" pageEncoding="UTF-8" session="false" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Genie</title>
    <link rel="shortcut icon" href="images/nficon2014.4.ico">
    <link rel="icon" href="images/nficon2014.4.ico">
    <link rel="stylesheet" href="bootstrap/css/bootstrap.css"/>
    <link rel="stylesheet" href="css/select2.css"/>
    <link rel="stylesheet" href="css/select2-bootstrap.css"/>
    <link rel="stylesheet" href="css/jquery.dataTables.min.css"/>
    <link rel="stylesheet" href="css/dataTables.bootstrap.css"/>
    <link rel="stylesheet" href="css/genie.css"/>
    <script src="vendor/js/require.js"></script>
    <script>
        requirejs.config({
            baseUrl: './js', paths: {
                'bootstrap': '../bootstrap/js/bootstrap',
                'underscore': '../vendor/js/lodash.min',
                'pager': '../vendor/js/pager',
                'knockout': '../vendor/js/knockout-2.2.1',
                'knockout.mapping': '../vendor/js/knockout.mapping',
                'jquery': '../vendor/js/jquery.min-2.0.2',
                'text': '../vendor/js/text',
                'stringTemplateEngine': '../vendor/js/stringTemplateEngine',
                'select2': '../vendor/js/select2',
                'loadKoTemplate': '../vendor/js/loadKoTemplate',
                'jqdatatables': '../vendor/js/jquery.dataTables.min',
                'dtbootstrap': '../vendor/js/dataTables.bootstrap.min',
                'moment': '../vendor/js/moment.min',
                'momentDurationFormat': '../vendor/js/moment-duration-format'
            },
            shim: {
                'bootstrap': ['jquery'],
                'select2': ['jquery'],
                'momentDurationFormat': ['moment']
            }
        });
    </script>
    <script>
        require(['appViewModel', 'bootstrap', 'select2']);
    </script>
</head>
<body>
<div class="navbar navbar-inverse navbar-fixed-top">
    <div class="navbar-inner">
        <div class="container">
            <a class="brand" href="#">GENIE</a>
            <ul class="nav" role="navigation">
                <li data-bind="css: {active: $__page__.find('job').isVisible}"><a href="#job/search">Jobs</a></li>
                <li data-bind="css: {active: $__page__.find('cluster').isVisible}"><a
                        href="#cluster/search">Clusters</a></li>
                <li data-bind="css: {active: $__page__.find('command').isVisible}"><a
                        href="#command/search">Commands</a></li>
                <li data-bind="css: {active: $__page__.find('application').isVisible}"><a
                        href="#application/search">Applications</a>
                </li>
            </ul>
        </div>
    </div>
</div>

<div class="container">
    <div data-bind="page: {id: 'start', beforeShow: function(page) { $root.Job.startup(); $root.Cluster.startup(); $root.Command.startup(); $root.Application.startup(); }}">
        <div data-bind="template: {name: 'front-page-html'}"></div>
    </div>
    <div data-bind="page: {id: 'job'}">
        <div data-bind="page: {id: 'search', afterHide: $root.scrollTop}">
            <div data-bind="template: {name: 'job-search-form-html'}"></div>
            <div data-bind="template: {name: 'job-search-results-html'}"></div>
        </div>
        <div data-bind="page: {id: 'details', afterHide: $root.scrollTop}">
        </div>
    </div>
    <div data-bind="page: {id: 'cluster'}">
        <div data-bind="page: {id: 'search', afterHide: $root.scrollTop}">
            <div data-bind="template: {name: 'cluster-search-form-html'}"></div>
            <div data-bind="template: {name: 'cluster-search-results-html'}"></div>
        </div>
        <div data-bind="page: {id: 'details', beforeShow: function(page) { $root.Cluster.update(); $root.scrollTop(); }, afterHide: $root.scrollTop}">
            <div data-bind="page: {id: '?', beforeShow: $root.Cluster.update, afterHide: function(page) { $root.Cluster.update(); $root.scrollTop(); }}">
                <div data-bind="template: {name: 'cluster-details-html', data: $root.Cluster.current}"></div>
            </div>
        </div>
        <div data-bind="page: {id: 'register', afterHide: $root.scrollTop}">
            <div data-bind="template: {name: 'cluster-register-form-html'}"></div>
        </div>
    </div>
    <div data-bind="page: {id: 'command'}">
        <div data-bind="page: {id: 'search', afterHide: $root.scrollTop}">
            <div data-bind="template: {name: 'command-search-form-html'}"></div>
            <div data-bind="template: {name: 'command-search-results-html'}"></div>
        </div>
        <div data-bind="page: {id: 'details', beforeShow: function(page) { $root.Command.update(); $root.scrollTop(); }, afterHide: $root.scrollTop}">
            <div data-bind="page: {id: '?', beforeShow: $root.Command.update, afterHide: function(page) { $root.Command.update(); $root.scrollTop(); }}">
                <div data-bind="template: {name: 'command-details-html', data: $root.Command.current}"></div>
            </div>
        </div>
    </div>
    <div data-bind="page: {id: 'application'}">
        <div data-bind="page: {id: 'search', afterHide: $root.scrollTop}">
            <div data-bind="template: {name: 'application-search-form-html'}"></div>
            <div data-bind="template: {name: 'application-search-results-html'}"></div>
        </div>
        <div data-bind="page: {id: 'details', beforeShow: function(page) { $root.Application.update(); $root.scrollTop(); }, afterHide: $root.scrollTop}">
            <div data-bind="page: {id: '?', beforeShow: $root.Application.update, afterHide: function(page) { $root.Application.update(); $root.scrollTop(); }}">
                <div data-bind="template: {name: 'application-details-html', data: $root.Application.current}"></div>
            </div>
        </div>
    </div>
</div>
<!-- /container -->
</body>
<jsp:include page="footer.jsp"/>
</html>
