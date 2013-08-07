define([
    'jquery',
    'underscore',
    'knockout',
    'knockout.mapping',
    'pager',
    'loadKoTemplate!../templates/cluster-search-form.html',
    'loadKoTemplate!../templates/cluster-search-results.html'
], function($, _, ko, mapping, pager) {
    ko.mapping = mapping;

    function Cluster(json) {
        var self = this;
        self.adHoc            = ko.observable();
        self.bonus            = ko.observable();
        self.createTime       = ko.observable();
        self.hadoopVersion    = ko.observable();
        self.hasStats         = ko.observable();
        self.id               = ko.observable();
        self.jobFlowId        = ko.observable();
        self.name             = ko.observable();
        self.prod             = ko.observable();
        self.prodHiveConfigId = ko.observable();
        self.prodPigConfigId  = ko.observable();
        self.s3CoreSiteXml    = ko.observable();
        self.s3HdfsSiteXml    = ko.observable();
        self.s3MapredSiteXml  = ko.observable();
        self.sla              = ko.observable();
        self.status           = ko.observable();
        self.test             = ko.observable();
        self.testHiveConfigId = ko.observable();
        self.testPigConfigId  = ko.observable();
        self.unitTest         = ko.observable();
        self.updateTime       = ko.observable();
        self.user             = ko.observable();

        ko.mapping.fromJS(json, {}, self);

        self.createTimeFormatted = ko.computed(function() {
            if (self.createTime() > 0) {
                var myDate = new Date(parseInt(self.createTime()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.statusClass = ko.computed(function() {
            if (self.status().toUpperCase() === 'UP') {
                //return 'text-success';
                return 'label-success';
            }
            else if (self.status().toUpperCase() === 'OUT_OF_SERVICE') {
                //return 'text-error';
                return 'label-warning';
            }
            return '';
        }, self);
    };

    var ClusterViewModel = function() {
        this.Cluster = {};
        var self = this.Cluster;
        self.searchResults   = ko.observableArray();
        self.runningClusters = ko.observableArray();

        self.startup = function() {
            self.runningClusters([]);
            $.ajax({
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  './genie/v0/config/cluster',
                data: {limit: 100000, status: 'UP'}
            }).done(function(data) {
                _.each(data.clusterConfigs.clusterConfig, function(clusterObj, index) {
                    self.runningClusters.push(clusterObj.name);
                });
                self.runningClusters(self.runningClusters().sort());
            }).fail(function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR);
            });
        };

        self.update = function() {
            self.searchResults([]);
            var formArray = $('#clusterSearchForm').serializeArray();
            var name     = _.where(formArray, {'name': 'name'})[0].value;
            var status   = _.where(formArray, {'name': 'status'})[0].value;
            var adHoc    = _.where(formArray, {'name': 'adHoc'})[0].value;
            var sla      = _.where(formArray, {'name': 'sla'})[0].value;
            var bonus    = _.where(formArray, {'name': 'bonus'})[0].value;
            var prod     = _.where(formArray, {'name': 'prod'})[0].value;
            var test     = _.where(formArray, {'name': 'test'})[0].value;
            var unitTest = _.where(formArray, {'name': 'unitTest'})[0].value;
            var hasStats = _.where(formArray, {'name': 'hasStats'})[0].value;
            var limit    = _.where(formArray, {'name': 'limit'})[0].value;
            $.ajax({
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  './genie/v0/config/cluster',
                data: {limit: limit, name: name, status: status, adHoc: adHoc, sla: sla, bonus: bonus, prod: prod, test: test, unitTest: unitTest, hasStats: hasStats}
            }).done(function(data) {
                _.each(data.clusterConfigs.clusterConfig, function(clusterObj, index) {
                    self.searchResults.push(new Cluster(clusterObj));
                });
            }).fail(function(jqXHR, textStatus, errorThrown) {
                if (jqXHR.responseText = '{"errorMsg":"No jobs found for specified criteria"}') {
                    console.log('NO CLUSTERS FOUND!');
                } else {
                    console.log(jqXHR);
                }
            });
        };
    };

    return ClusterViewModel;
});


