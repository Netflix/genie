define([
    'jquery',
    'underscore',
    'knockout',
    'knockout.mapping',
    'pager',
    'loadKoTemplate!../templates/job-search-form.html',
    'loadKoTemplate!../templates/job-search-results.html'
], function($, _, ko, mapping, pager) {
    ko.mapping = mapping;

    function Job(json) {
        var self = this;
        self.archiveLocation  = ko.observable();
        self.clientHost       = ko.observable();
        self.executionClusterId        = ko.observable();
        self.executionClusterName      = ko.observable();
        self.commandArgs          = ko.observable();
        self.configuration    = ko.observable();
        self.exitCode         = ko.observable();
        self.fileDependencies = ko.observable();
        self.finishTime       = ko.observable();
        self.forwarded        = ko.observable();
        self.hostName         = ko.observable();
        self.id            = ko.observable();
        self.name          = ko.observable();
        self.commandId          = ko.observable();
        self.commandName          = ko.observable();
        self.applicationId          = ko.observable();
        self.applicationName          = ko.observable();
        self.killURI          = ko.observable();
        self.outputURI        = ko.observable();
        self.pigVersion       = ko.observable();
        self.processHandle    = ko.observable();
        self.schedule         = ko.observable();
        self.startTime        = ko.observable();
        self.status           = ko.observable();
        self.statusMsg        = ko.observable();
        self.updated       = ko.observable();
        self.created = ko.observable();
        self.user         = ko.observable();

        ko.mapping.fromJS(json, {}, self);

        self.nameFormatted = ko.computed(function() {
            var nameLength = self.name() ? self.name().length : -1;
            if (nameLength > 28) {
                return self.name().substring(0,20) + '...' + self.name().substring(nameLength-8);
            }
            return self.name();
        }, self);

        self.finishTimeFormatted = ko.computed(function() {
            if (self.finishTime() > 0) {
                var myDate = new Date(parseInt(self.finishTime()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.startTimeFormatted = ko.computed(function() {
            //if (self.startTime() > 0) {
            if (self.created > 0) { 	
                var myDate = new Date(parseInt(self.created()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.updateTimeFormatted = ko.computed(function() {
            if (self.updated() > 0) {
                var myDate = new Date(parseInt(self.updated()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.statusClass = ko.computed(function() {
            if (self.status().toUpperCase() === 'SUCCEEDED') {
                //return 'text-success';
                return 'label-success';
            }
            else if (self.status().toUpperCase() === 'FAILED') {
                //return 'text-error';
                return 'label-important';
            }
            return '';
        }, self);
    };

    var JobViewModel = function() {
        this.Job = {};
        var self = this.Job;
        self.status = ko.observable('');
        self.searchResults = ko.observableArray();
        self.searchDateTime = ko.observable();
        self.runningJobs = ko.observableArray();
        self.runningJobCount = ko.computed(function() {
            return _.reduce(self.runningJobs(), function(sum, obj, index) { return sum + obj.count; }, 0);
        }, self);

        self.startup = function() {
            self.runningJobs([]);
            var jobCount = {};
            $.ajax({
                global: false,
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v2/jobs',
                data: {limit: 64, status: 'RUNNING'}
            }).done(function(data) {
                // check to see if jobInfo is an array
                if (data instanceof Array) {
                    _.each(data, function(jobObj, index) {
                        if (!(jobObj.jobType in jobCount)) {
                            jobCount[jobObj.jobType] = 0;
                        }
                        jobCount[jobObj.jobType] += 1;
                    });
                } else {
                    if (!(data.jobs.jobInfo.jobType in jobCount)) {
                        jobCount[data.jobs.jobInfo.jobType] = 0;
                    }
                    jobCount[data.jobs.jobInfo.jobType] += 1;
                }
                _.each(jobCount, function(count, type) {
                    self.runningJobs.push({type: type, count: count});
                });
            });
        };

        self.search = function() {
            var d = new Date();
            self.searchResults([]);
            self.status('searching');
            self.searchDateTime(d.toLocaleString());
            var formArray = $('#jobSearchForm').serializeArray();
            var user = _.where(formArray, {'name': 'userName'})[0].value;
            var status   = _.where(formArray, {'name': 'status'})[0].value;
            var id    = _.where(formArray, {'name': 'jobID'})[0].value;
            var name  = _.where(formArray, {'name': 'jobName'})[0].value;
            var executionClusterName  = _.where(formArray, {'name': 'clusterName'})[0].value;
            var executionClusterId  = _.where(formArray, {'name': 'clusterId'})[0].value;
            var limit    = _.where(formArray, {'name': 'limit'})[0].value;
            $.ajax({
                global: false,
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v2/jobs',
                data: {limit: limit, userName: user, status: status, 
                    id: id, name: name, executionClusterName:executionClusterName, executionClusterId:executionClusterId}
            }).done(function(data) {
                self.searchResults([]);
                self.status('results');
                // check to see if jobInfo is an array
                if (data instanceof Array) {
                    _.each(data, function(jobObj, index) {
                        if (! jobObj.name) {
                            jobObj.name = 'undefined';
                        }
                        self.searchResults.push(new Job(jobObj));
                    });
                } else {
                    if (! data.jobs.jobInfo.name) {
                        data.jobs.jobInfo.name = 'undefined';
                    }
                    self.searchResults.push(new Job(data));                
                }
            }).fail(function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR, textStatus, errorThrown);
                self.status('results');
            });
        };
    };

    return JobViewModel;
});
