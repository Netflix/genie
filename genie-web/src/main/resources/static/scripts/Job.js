import React from 'react';

import Page from './Page';

import JobDetails from './components/JobDetails';
import JobTableBody from './components/JobTableBody';
import TableHeader from './components/TableHeader';

import { fetch } from './utils';

export default class Job extends Page {

  get url() {
    return '/api/v3/jobs';
  }

  get dataKey() {
    return 'jobSearchResultList';
  }

  get formFields() {
    return [
      {
        label : 'User Name',
        name  : 'user',
        value : '',
        type  : 'input',
      }, {
        label : 'Job Id',
        name  : 'id',
        value : '',
        type  : 'input',
      }, {
        label : 'Job Name',
        name  : 'name',
        value : '',
        type  : 'input',
      }, {
        label : 'Status',
        name  : 'status',
        value : '',
        type  : 'option',
        optionValues: ['', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED'],
      }, {
        label : 'Size',
        name  : 'size',
        value : 25,
        type  : 'option',
        optionValues: [10, 25, 50, 100],
      }, {
        label : 'Sort By',
        name  : 'sort',
        value : '',
        type  : 'select',
        selectFields: ['user', 'created', 'id', 'name', 'status', 'clusterName', 'cluserId'].map((field) => {
          return {
            value: field,
            label: field,
          };
        }),
      },
    ];
  }

  get hiddenFormFields() {
    return [
      {
        label : 'Tags',
        name  : 'tag',
        value : '',
        type  : 'input',
      }, {
        label : 'Cluster Name',
        name  : 'clusterName',
        value : '',
        type  : 'input',
      }, {
        label : 'Cluster ID',
        name  : 'cluserId',
        value : '',
        type  : 'input',
      }, {
        label : 'Command Name',
        name  : 'commandName',
        value : '',
        type  : 'input',
      }, {
        label : 'Command ID',
        name  : 'commandId',
        value : '',
        type  : 'input',
      },
    ];
  }

  get searchPath() {
    return 'jobs';
  }

  get tableHeader() {
    return (
      <TableHeader
        headers={['Id', 'Name', 'User', 'Status', 'Cluster', 'Output', 'Started', 'Finished', 'Run Time']}
      />
    );
  }

  killJob = (jobId) => {
    fetch(`/api/v3/jobs/${jobId}`, null, 'DELETE')
      .done(() => {
        this.setState({ killJobRequestSent: true });
      });
  }

  get tableBody() {
    const { showDetails } = this.props.location.query;
    return (
      <JobTableBody
        rows={this.state.data}
        rowId={showDetails}
        setRowId={this.setRowId}
        detailsTable={JobDetails}
        killJob={this.killJob}
        killJobRequestSent={this.state.killJobRequestSent}
        hideDetails={this.hideDetails}
      />
    );
  }
}
