import React from 'react';
import { Link } from 'react-router';
import { render } from 'react-dom';

import { momentFormat } from '../utils';

import TableHeader from './TableHeader';
import TableBody from './TableBody';
import TableRow from './TableRow';

import ClusterDetails from './ClusterDetails';

export default class ClusterTableBody extends TableBody {
  constructor(props) {
    super(props);
  }

  render() {
    let tableRows = this.construct(TableRow);

    if (this.state.showDetails) {
      tableRows.splice(
        this.state.index + 1,
        0,
        <ClusterDetails
          clusterUrl={this.state.row._links.self.href}
          commandsUrl={this.state.row._links.commands.href}
          key="cluster-details"
          hideDetails={this.hideDetails}
        />
      );
    }

    return (
      <tbody>
        {tableRows}
      </tbody>
    );
  }
}
