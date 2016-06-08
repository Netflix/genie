import React from 'react';

import Page from './Page';

import TableHeader from './components/TableHeader';
import TableBody from './components/TableBody';

import CommandDetails from './components/CommandDetails';

export default class Command extends Page {

  get url() {
    return '/api/v3/commands';
  }

  get dataKey() {
    return 'commandList';
  }

  get formFields() {
    return [
      {
        label : 'Name',
        name  : 'name',
        value : '',
        type  : 'input',
      }, {
        label : 'User',
        name  : 'user',
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
        selectFields: ['name', 'user', 'status', 'tag'].map(field => {
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
        detailsTable={CommandDetails}
        hideDetails={this.hideDetails}
      />
    );
  }
}

