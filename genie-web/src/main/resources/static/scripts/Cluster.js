import React from 'react';

import BasePage from './BasePage';
import SearchResult from './components/SearchResult';
import NoSearchResult from './components/NoSearchResult';
import SearchBar from './components/SearchBar';
import SearchForm from './components/SearchForm';

import TableBody from './components/ClusterTableBody';
import TableHeader from './components/TableHeader';

import { fetch } from './utils';

export default class Cluster extends BasePage {

  constructor(props) {
    super(props);
    this.state = {
      clusters       : [],
      links          : {},
      page           : {},
      showSearchForm : true,
    };
  }

  loadData(query) {
    fetch('/api/v3/clusters', query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          query,
          noSearchResult : false,
          page           : data.page,
          links          : data._links,
          clusters       : data._embedded.clusterList,
        });
      } else {
        this.setState({
          query,
          clusters  : [],
        });
      }
    });
  }

  render() {
    let search = this.state.showSearchForm ?
      <SearchForm
        query={this.state.query}
        toggleSearchForm={this.toggleSearchForm}
        formFields={[
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
        ]}
        searchPath="clusters"
      /> :
      <SearchBar toggleSearchForm={this.toggleSearchForm} />;

    let searchResult = this.state.clusters.length > 0 ?
      <SearchResult
        data={this.state.clusters}
        links={this.state.links}
        page={this.state.page}
        pageType="clusters"
        headers={['Id', 'Name', 'User', 'Status', 'Version', 'Tags', 'Created', 'Updated']}
        showSearchForm={this.state.showSearchForm}
        TableBody={TableBody}
        TableHeader={TableHeader}
      /> :
      <NoSearchResult />;

    return (
      <div className="row" >
        {search}
        {searchResult}
      </div>
    );
  }
}
