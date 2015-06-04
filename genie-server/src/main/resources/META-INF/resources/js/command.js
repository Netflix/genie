define([
    'jquery',
    'underscore',
    'knockout',
    'knockout.mapping',
    'pager',
    'moment',
    'momentDurationFormat',
    'loadKoTemplate!../templates/command-search-form.html',
    'loadKoTemplate!../templates/command-search-results.html',
    'loadKoTemplate!../templates/command-details.html'
], function($, _, ko, mapping, pager, moment) {
    ko.mapping = mapping;

    function Command(json) {
        var self = this;
        self.objStatus        = ko.observable('ready');
        self.created      = ko.observable();
        self.version    = ko.observable();
        self.id               = ko.observable();
        self.name             = ko.observable();
        self.status           = ko.observable();
        self.updated      = ko.observable();
        self.user             = ko.observable();
        self.jobType = ko.observable();
        self.configs = ko.observableArray();
        self.tags = ko.observableArray();
        self.clusters = ko.observableArray();
        self.application = ko.observable();
        self.createTimeFormatted = ko.observable();
        self.updateTimeFormatted = ko.observable();
        self.formattedTags = ko.observable();

        ko.mapping.fromJS(json, {}, self);
        self.originalStatus = self.status();

        self.idFormatted = ko.computed(function() {
            var idLength = self.id() ? self.id().length : -1;
            if (idLength > 30) {
                return self.id().substring(0,20) + '...' + self.id().substring(idLength-10);
            }
            return self.id();
        }, self);

        self.applicationId = ko.computed(function() {
            if (self.application() !== undefined) {
                return self.application().id;
            }
            return '';
        },self);

        self.applicationName = ko.computed(function() {
            if (self.application() !== undefined) {
                return self.application().name;
            }
            return '';
        },self);

        self.statusClass = ko.computed(function() {
            if (self.status() && self.status().toUpperCase() === 'ACTIVE') {
                return 'label-success';
            }
            return '';
        }, self);

        self.updateStatus = function() {
            self.objStatus('updating');
            $.ajax({
                type: 'PUT',
                headers: {'content-type':'application/json', 'Accept':'application/json'},
                url: 'genie/v2/config/commands/'+self.id(),
                data: JSON.stringify({status: self.status()})
            }).done(function(data) {
                self.objStatus('ready');
                location.reload(true);
            }).fail(function(jqXHR, textStatus, errorThrown) {
                self.objStatus('ready');
                self.status(self.originalStatus);
            });
        };
    };

    var CommandViewModel = function() {
        this.Command = {};
        var self = this.Command;
        self.status = ko.observable('');
        self.current = ko.observable(new Command());
        self.searchResults = ko.observableArray();
        self.searchDateTime = ko.observable();
        self.runningCommands = ko.observableArray();
        self.allTags = ko.observableArray();
        self.selectedTags = ko.observableArray();
        self.displayForm = ko.observable(true);

        self.commandOrderByFields = ko.observableArray(['user','started','created','id','name','status']);
        self.commandOrderBySelectedFields = ko.observableArray();
        
        self.runningCommandCount = ko.computed(function() {
            return _.reduce(self.runningCommands(), function(sum, obj, index) { return sum + obj.count; }, 0);
        }, self);
        
        self.startup = function() {
            self.runningCommands([]);
            var commandCount = {};
            $.ajax({
                global: false,
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v2/config/commands?status=ACTIVE'
            }).done(function(data) {
            	if (data instanceof Array) {
                    _.each(data, function(commandObj, index) {
                        if (!(commandObj.status in commandCount)) {
                            commandCount[commandObj.status] = 0;
                        }
                        commandCount[commandObj.status] += 1;
                        _.each(commandObj.tags, function(tag, index) {
                        	if (self.allTags.indexOf(tag) < 0) {
                        		self.allTags.push(tag);
                        	}
                        });
                    });
                    $("#commandSearchTags").select2();
                } else {
                    var commandObj = data;
                    if (!(commandObj.status in commandCount)) {
                        commandCount[commandObj.status] = 0;
                    }
                    commandCount[commandObj.status] += 1                    
                }
                _.each(commandCount, function(count, status) {
                    self.runningCommands.push({status: status, count: count});
                });
                $("#commandOrderFields").select2();
            });
        };

        self.showForm = function() {
            self.displayForm(true);
        }

        self.hideForm = function() {
            self.displayForm(false);
        }

        self.search = function() {
            var d = new Date();
            self.searchResults([]);
            self.status('searching');
            self.searchDateTime(d.toLocaleString());
            
            var formArray = $('#commandSearchForm').serializeArray();
            var name     = _.where(formArray, {'name': 'name'})[0].value;
            var status   = _.where(formArray, {'name': 'status'})[0].value;
            var limit    = _.where(formArray, {'name': 'limit'})[0].value;

            var sortOrder = _.where(formArray, {'name': 'sortOrder'})[0].value;
            var bDescending =  true;

            if (sortOrder != 'descending') {
                bDescending = false;
            }

            $.ajax({
                global: false,
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v2/config/commands',
                traditional: true,
                data: {limit: limit, name: name, status: status, tag: self.selectedTags(), orderBy: self.commandOrderBySelectedFields(), descending: bDescending }
            }).done(function(data) {
            	self.searchResults([]);
                self.status('results');
                self.displayForm(false);
                if (data instanceof Array) {
                    _.each(data, function(commandObj, index) {

                        commandObj.idLink  = $("<div />").append($("<a />", {
                            href : '/#command/details/'+commandObj.id,
                            target: "_blank"
                        }).append($("<img/>", {src: '../images/folder.svg', class: 'open-icon'}))).html();

                        commandObj.rawLink  = $("<div />").append($("<a />", {
                            href : "genie/v2/config/commands/" + commandObj.id,
                            target: "_blank"
                        }).append($("<img/>", {src: '../images/genie.gif', class: 'genie-icon'}))).html();

                        var createdDt = new Date(commandObj.created);
                        commandObj.createTimeFormatted = moment(createdDt).format('MM/DD/YYYY HH:mm:ss');

                        var updatedDt = new Date(commandObj.updated);
                        commandObj.updateTimeFormatted = moment(updatedDt).format('MM/DD/YYYY HH:mm:ss');

                        commandObj.formattedTags = commandObj.tags.join("<br />");

                        self.searchResults.push(new Command(commandObj));
                    });
                } else {
                    self.searchResults.push(new Command(data));
                }

                var table = $("#commandDataTable").DataTable ();
                table.destroy();
                $("#commandDataTable").DataTable ( {
                        data: self.searchResults(),
                        "aaSorting": [],
                        "oLanguage": {
                            "sSearch": "Filter Results: "
                        },
                        columns: [
                            { title: 'Id', data: 'id' },
                            { title: 'Name', data: 'name' },
                            { title: 'User', data: 'user', className: "dt-center"},
                            { title: 'Version', data: 'version', className: "dt-center"},
                            { title: 'Tags', data: 'formattedTags'},
                            { title: 'Create Time (UTC)', data: 'createTimeFormatted', className: "dt-center"},
                            { title: 'Update Time (UTC)', data: 'updateTimeFormatted', className: "dt-center"},
                            { title: 'Details', data: 'idLink', className: "dt-center"},
                            { title: 'JSON', data: 'rawLink', className: "dt-center"},
                            { title: 'Status', name: 'status', data: 'status', className: "dt-center"}
                        ],
                        // TODO use names of datatable columns to change class instead of static index 10
                        "createdRow": function ( row, data, index ) {
                            if (data.status() == 'ACTIVE') {
                                $('td', row).eq(9).addClass('text-success');
                            } else if (data.status() == 'INACTIVE') {
                                $('td', row).eq(9).addClass('text-error');
                            }
                        }
                    }
                )

            }).fail(function(jqXHR, textStatus, errorThrown) {
                console.log(jqXHR, textStatus, errorThrown);
                self.status('results');
            });
        };

        self.update = function(page) {
            if (page) {
                var commandId = page.page.currentId;
                $.ajax({
                    type: 'GET',
                    headers: {'Accept':'application/json'},
                    url:  'genie/v2/config/commands/'+commandId
                }).done(function(commandObj) {
                	//console.log(command);

                    var createdDt = new Date(commandObj.created);
                    commandObj.createTimeFormatted = moment(createdDt).format('MM/DD/YYYY HH:mm:ss');

                    var updatedDt = new Date(commandObj.updated);
                    commandObj.updateTimeFormatted = moment(updatedDt).format('MM/DD/YYYY HH:mm:ss');

                    commandObj.formattedTags = commandObj.tags.join("<br />");

                    self.current(new Command(commandObj));
                    $.ajax({
                        type: 'GET',
                        headers: {'Accept':'application/json'},
                        url:  'genie/v2/config/commands/'+commandId+'/clusters?status=UP'
                    }).done(function(clusters) {
                        self.current().clusters(clusters);
                    });

                    $.ajax({
                        type: 'GET',
                        headers: {'Accept':'application/json'},
                        url:  'genie/v2/config/commands/'+commandId+'/application'
                    }).done(function(application) {
                        console.log("Application: " + application);
                        self.current().application(application);
                    }).fail(function(jqXHR, textStatus, errorThrown) {
                        console.log(jqXHR, textStatus, errorThrown);
                        var application = {id:"None", name:"None"};
                        self.current().application(application);
                    });
                });
            } else {
                self.current(new Command());
            }
        };

    };

    return CommandViewModel;
});
