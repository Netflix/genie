import React from 'react';

import TableBody from './TableBody';
import TableRow from './TableRow';

import ClusterDetails from './ClusterDetails';

export default class ClusterTableBody extends TableBody {

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
