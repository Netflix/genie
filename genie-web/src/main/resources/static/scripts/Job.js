import React from 'react';

import BasePage from './BasePage';
import SearchResult from './components/SearchResult';
import NoSearchResult from './components/NoSearchResult';
import JobSearchForm from './components/JobSearchForm';
import SearchBar from './components/SearchBar';

import JobTableBody from './components/JobTableBody';
import TableHeader from './components/TableHeader';

import { fetch } from './utils';

export default class Job extends BasePage {

  constructor(props) {
    super(props);
    this.state = {
      jobs           : [],
      links          : {},
      page           : {},
      showSearchForm : true,
    };
  }

  loadData(query) {
    fetch('/api/v3/jobs', query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          query,
          noSearchResult : false,
          page           : data.page,
          links          : data._links,
          jobs           : data._embedded.jobSearchResultList,
        });
      } else {
        this.setState({
          query,
          jobs  : [],
        });
      }
    });
  }

  render() {
    let jobSearch = this.state.showSearchForm ?
      <JobSearchForm
        query={this.state.query}
        toggleSearchForm={this.toggleSearchForm}
      /> :
      <SearchBar toggleSearchForm={this.toggleSearchForm} />;

    let jobSearchResult = this.state.jobs.length > 0 ?
      <SearchResult
        data={this.state.jobs}
        links={this.state.links}
        page={this.state.page}
        pageType="jobs"
        headers={['Id', 'Name', 'User', 'Status', 'Cluster', 'Output', 'Started', 'Finished', 'Run Time']}
        showSearchForm={this.state.showSearchForm}
        TableBody={JobTableBody}
        TableHeader={TableHeader}
      /> :
      <NoSearchResult />;

    return (
      <div className="row" >
        {jobSearch}
        {jobSearchResult}
      </div>
    );
  }
}
