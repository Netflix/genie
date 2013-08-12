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
        self.clusterId        = ko.observable();
        self.clusterName      = ko.observable();
        self.cmdArgs          = ko.observable();
        self.configuration    = ko.observable();
        self.exitCode         = ko.observable();
        self.fileDependencies = ko.observable();
        self.finishTime       = ko.observable();
        self.forwarded        = ko.observable();
        self.hostName         = ko.observable();
        self.jobID            = ko.observable();
        self.jobName          = ko.observable();
        self.jobType          = ko.observable();
        self.killURI          = ko.observable();
        self.outputURI        = ko.observable();
        self.pigVersion       = ko.observable();
        self.processHandle    = ko.observable();
        self.schedule         = ko.observable();
        self.startTime        = ko.observable();
        self.status           = ko.observable();
        self.statusMsg        = ko.observable();
        self.updateTime       = ko.observable();
        self.userName         = ko.observable();

        ko.mapping.fromJS(json, {}, self);

        self.jobNameFormatted = ko.computed(function() {
            var nameLength = self.jobName() ? self.jobName().length : -1;
            if (nameLength > 28) {
                return self.jobName().substring(0,20) + '...' + self.jobName().substring(nameLength-8);
            }
            return self.jobName();
        }, self);

        self.finishTimeFormatted = ko.computed(function() {
            if (self.finishTime() > 0) {
                var myDate = new Date(parseInt(self.finishTime()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.startTimeFormatted = ko.computed(function() {
            if (self.startTime() > 0) {
                var myDate = new Date(parseInt(self.startTime()));
                return myDate.toUTCString();
            }
            return '';
        }, self);

        self.updateTimeFormatted = ko.computed(function() {
            if (self.updateTime() > 0) {
                var myDate = new Date(parseInt(self.updateTime()));
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
        self.status = ko.observable('hidden');
        self.searchResults = ko.observableArray();
        self.runningJobs   = ko.observableArray();
        self.runningJobCount = ko.computed(function() {
            return _.reduce(self.runningJobs(), function(sum, obj, index) { return sum + obj.count; }, 0);
        }, self);

        self.startup = function() {
            self.runningJobs([]);
            var jobCount = {};
            $.ajax({
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v0/jobs',
                data: {limit: 64, status: 'RUNNING'}
            }).done(function(data) {
                // check to see if jobInfo is an array
                if (data.jobs.jobInfo instanceof Array) {
                    _.each(data.jobs.jobInfo, function(jobObj, index) {
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
            }).fail(function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR);
            });
        };

        self.update = function() {
            self.searchResults([]);
            var formArray = $('#jobSearchForm').serializeArray();
            var userName = _.where(formArray, {'name': 'userName'})[0].value;
            var jobType  = _.where(formArray, {'name': 'jobType'})[0].value;
            var status   = _.where(formArray, {'name': 'status'})[0].value;
            var jobID    = _.where(formArray, {'name': 'jobID'})[0].value;
            var jobName  = _.where(formArray, {'name': 'jobName'})[0].value;
            var limit    = _.where(formArray, {'name': 'limit'})[0].value;
            $.ajax({
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v0/jobs',
                data: {limit: limit, userName: userName, jobType: jobType, status: status, jobID: jobID, jobName: jobName}
            }).done(function(data) {
                // check to see if jobInfo is an array
                if (data.jobs.jobInfo instanceof Array) {
                    _.each(data.jobs.jobInfo, function(jobObj, index) {
                        if (! jobObj.jobName) {
                            jobObj.jobName = 'undefined';
                        }
                        self.searchResults.push(new Job(jobObj));
                    });
                } else {
                    if (! data.jobs.jobInfo.jobName) {
                        data.jobs.jobInfo.jobName = 'undefined';
                    }
                    self.searchResults.push(new Job(data.jobs.jobInfo));                
                }
            }).fail(function(jqXHR, textStatus, errorThrown) {
                self.status('visible');
                if (jqXHR.responseText = '{"errorMsg":"No jobs found for specified criteria"}') {
                    console.log('No jobs found!');
                } else {
                    console.log(jqXHR);
                }
            });
        };
    };

    return JobViewModel;
});


