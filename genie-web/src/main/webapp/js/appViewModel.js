// Main viewmodel class
define([
    'jquery',
    'knockout',
    'knockout.mapping',
    'pager',
    'underscore',
    'job',
    'cluster',
    'text!../templates/error.html',
    'loadKoTemplate!../templates/front-page.html'
], function($, ko, mapping, pager, _, Job, Cluster, errorTemplate) {
    $(function() {
        function appViewModel() {
            var self = this;
            Job.call(self);
            Cluster.call(self);
            self.page = ko.observable("index");

            self.scrollTop = function() {
                scrollTo(0,0);
            };
        };

        var viewModel = new appViewModel();
        foo = viewModel;

        pager.extendWithPage(viewModel);
        ko.applyBindings(viewModel);
        pager.start();
        viewModel.page(window.location.hash);

        $(document).ajaxError(function(event, jqXHR, settings, exception) {
            console.log(event, jqXHR, settings, exception);
            var data = {
                status: jqXHR.status,
                statusText: jqXHR.statusText,
                responseText: jqXHR.responseText,
                type: settings.type,
                url: settings.url
            };
            var errorHtml = _.template(errorTemplate, data, {variable: 'data'});
            $('.error-message').remove();
            $('body > div.container').prepend(errorHtml);
        });

        $(window).on('hashchange', function(e) {
            viewModel.page(window.location.hash);
        });
    });
});
