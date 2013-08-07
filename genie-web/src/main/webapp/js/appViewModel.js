// Main viewmodel class
define(['jquery', 'knockout', 'knockout.mapping', 'pager', 'underscore', 'job', 'cluster'], function($, ko, mapping, pager, _, Job, Cluster) {
    $(function() {
        function appViewModel() {
            var self = this;
            Job.call(self);
            Cluster.call(self);
        };

        var viewModel = new appViewModel();
        foo = viewModel;

        pager.extendWithPage(viewModel);
        ko.applyBindings(viewModel);
        pager.start();
    });
});
