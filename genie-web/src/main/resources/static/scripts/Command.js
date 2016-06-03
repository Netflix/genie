import React from 'react';
import { fetch } from './utils';

import BasePage from './BasePage';
import SearchResult from './components/SearchResult';
import NoSearchResult from './components/NoSearchResult';
import SearchBar from './components/SearchBar';
import SearchForm from './components/SearchForm';

import TableBody from './components/CommandTableBody';
import TableHeader from './components/TableHeader';

export default class Command extends BasePage {

  constructor(props) {
    super(props);
    this.state = {
      commands       : [],
      links          : {},
      page           : {},
      showSearchForm : true,
    };
  }

  loadData(query) {
    fetch('/api/v3/commands', query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          query,
          noSearchResult : false,
          page           : data.page,
          links          : data._links,
          commands       : data._embedded.commandList,
        });
      } else {
        this.setState({
          query,
          commands : [],
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
        ]}
        searchPath="commands"
      /> :
      <SearchBar toggleSearchForm={this.toggleSearchForm} />;

    let searchResult = this.state.commands.length > 0 ?
      <SearchResult
        data={this.state.commands}
        links={this.state.links}
        page={this.state.page}
        pageType="commands"
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

