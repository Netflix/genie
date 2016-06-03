import React from 'react';

import TableBody from './TableBody';
import TableRow from './TableRow';

import CommandDetails from './CommandDetails';

export default class CommandTableBody extends TableBody {

  render() {
    let tableRows = this.construct(TableRow);

    if (this.state.showDetails) {
      tableRows.splice(
        this.state.index + 1,
        0,
        <CommandDetails
          commandUrl={this.state.row._links.self.href}
          clustersUrl={this.state.row._links.clusters.href}
          applicationsUrl={this.state.row._links.applications.href}
          key="command-details"
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
