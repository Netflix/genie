import React from 'react';
import { render } from 'react-dom';

import SearchResult from './components/SearchResult';
import NoSearchResult from './components/NoSearchResult';
import JobSearchForm from './components/JobSearchForm';
import SearchBar from './components/SearchBar';

import JobTableBody from './components/JobTableBody';
import TableHeader from './components/TableHeader';

import { fetch, hasChanged } from './utils';
import $ from 'jquery';

export default class Job extends React.Component {

  static childContextTypes = {
    location: React.PropTypes.object.isRequired
  }

  getChildContext() {
    return { location: this.props.location }
  }

  constructor(props) {
    super(props);
    this.state = {
      jobs           : [],
      links          : {},
      page           : {},
      showSearchForm : true,
    };
  }

  componentDidMount() {
    const { query } = this.props.location;
    this.loadData(query);
  }

  componentWillReceiveProps(nextProps) {
    const { query } = nextProps.location;
    if (hasChanged(query,
                   this.props.location.query)) {
      this.loadData(query);
    }
  }

  loadData(query) {
    if ($.isEmptyObject(query)) {
      query = {size: 25}
    }

    fetch('/api/v3/jobs', query)
    .done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          noSearchResult : false,
          query          : query,
          page           : data.page,
          links          : data._links,
          jobs           : data._embedded.jobSearchResultList,
        });
      }
      else {
        this.setState({
          query : query,
          jobs  : [],
        });
      }
    });
  }

  toggleSearchForm = () => {
    this.setState({showSearchForm: !this.state.showSearchForm});
  }

  render() {
    let jobSearch = this.state.showSearchForm ?
                    <JobSearchForm
                      query={this.state.query}
                      toggleSearchForm={this.toggleSearchForm}
                    />:
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
                          />:
                          <NoSearchResult />;


  return (
    <div className="row" >
      {jobSearch}
      {jobSearchResult}
    </div>
  );
  }
}
