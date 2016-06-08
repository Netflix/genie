import React from 'react';

import Page from './Page';

import TableHeader from './components/TableHeader';
import TableBody from './components/TableBody';

import ClusterDetails from './components/ClusterDetails';


export default class Cluster extends Page {

  get url() {
    return '/api/v3/clusters';
  }

  get dataKey() {
    return 'clusterList';
  }

  get formFields() {
    return [
      {
        label : 'Name',
        name  : 'name',
        value : '',
        type  : 'input',
      }, {
        label : 'Status',
        name  : 'status',
        value : '',
        type  : 'input',
      }, {
        label : 'Tag',
        name  : 'tag',
        value : '',
        type  : 'input',
      }, {
        label : 'Sort By',
        name  : 'sort',
        value : '',
        type  : 'select',
        selectFields: ['name', 'status', 'tag'].map(field => {
          return {
            value: field,
            label: field,
          };
        }),
      },
    ];
  }

  get searchPath() {
    return 'clusters';
  }

  get tableHeader() {
    return (
      <TableHeader
        headers={['Id', 'Name', 'User', 'Status', 'Version', 'Tags', 'Created', 'Updated']}
      />
    );
  }

  get tableBody() {
    const { showDetails } = this.props.location.query;
    return (
      <TableBody
        rows={this.state.data}
        rowId={showDetails}
        setRowId={this.setRowId}
        detailsTable={ClusterDetails}
        hideDetails={this.hideDetails}
      />
    );
  }
}
