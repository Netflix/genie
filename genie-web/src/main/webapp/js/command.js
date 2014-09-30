define([
    'jquery',
    'underscore',
    'knockout',
    'knockout.mapping',
    'pager',
    'loadKoTemplate!../templates/command-search-form.html',
    'loadKoTemplate!../templates/command-search-results.html',
    'loadKoTemplate!../templates/command-details.html'
], function($, _, ko, mapping, pager) {
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

        ko.mapping.fromJS(json, {}, self);
        self.originalStatus = self.status();

        self.idFormatted = ko.computed(function() {
            var idLength = self.id() ? self.id().length : -1;
            if (idLength > 30) {
                return self.id().substring(0,20) + '...' + self.id().substring(idLength-10);
            }
            return self.id();
        }, self);
        
        self.createTimeFormatted = ko.computed(function() {
            if (self.created() > 0) {
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
                data: JSON.stringify({commands: {user: self.user(), status: self.status()}})
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
                url:  'genie/v2/config/commands?status=ACTIVE',
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
            });
        };

        self.search = function() {
            var d = new Date();
            self.searchResults([]);
            self.status('searching');
            self.searchDateTime(d.toLocaleString());
            
            var formArray = $('#commandSearchForm').serializeArray();
            var name     = _.where(formArray, {'name': 'name'})[0].value;
            var status   = _.where(formArray, {'name': 'status'})[0].value;
            var limit    = _.where(formArray, {'name': 'limit'})[0].value;
            
            $.ajax({
                global: false,
                type: 'GET',
                headers: {'Accept':'application/json'},
                url:  'genie/v2/config/commands',
                traditional: true,
                data: {limit: limit, name: name, status: status, tag: self.selectedTags()}
            }).done(function(data) {
            	self.searchResults([]);
                self.status('results');
                if (data instanceof Array) {
                    _.each(data, function(commandObj, index) {
                        self.searchResults.push(new Command(commandObj));
                    });
                } else {
                    self.searchResults.push(new Command(data));
                }
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
                }).done(function(data) {
                	console.log(data);
                    self.current(new Command(data));
                });
            } else {
                self.current(new Command());
            }
        };

    };

    return CommandViewModel;
});
