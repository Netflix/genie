import React from 'react';
import { render } from 'react-dom';
import $ from 'jquery';
import JobSearchForm from './components/JobSearchForm';
import JobSearchResult from './components/JobSearchResult';
import NoSearchResult from './components/NoSearchResult';
import SearchBar from './components/SearchBar';

export default class Job extends React.Component {

  constructor(props) {
    super(props);
    this.state = this.getDefaultState();
  }

  getDefaultState() {
    return {
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
    this.loadData(query);
  }

  loadData(query) {
    if ($.isEmptyObject(query)) {
      query = { size: 25 };
    }

    $.ajax({
      global: false,
      type: 'GET',
      headers: {
      'Accept': 'application/hal+json',
      },
      url: '/api/v3/jobs',
      data: query,
    }).done((data) => {
      if (data.hasOwnProperty('_embedded')) {
        this.setState({
          jobs           : data._embedded.jobSearchResultList,
          links          : data._links,
          page           : data.page,
          noSearchResult : false,
          query          : query,
        });
      }
      else {
        this.setState({
          query : query,
          jobs  : []
        });
      }
    });
  }

  toggleSearchForm = () => {
    this.setState({showSearchForm: !this.state.showSearchForm});
  }

  render() {
    let jobSearchResult = this.state.jobs.length > 0 ?
                          <JobSearchResult
                            jobs={this.state.jobs}
                            links={this.state.links}
                            page={this.state.page}
                            headers={['Id', 'Name', 'User', 'Status', 'Cluser', 'Output', 'Started', 'Finished', 'Run Time']}
                            showSearchForm={this.state.showSearchForm}
                          />:
                          <NoSearchResult />;

    let jobSearch = this.state.showSearchForm ?
                    <JobSearchForm
                      query={this.state.query}
                      toggleSearchForm={this.toggleSearchForm}
                    />:
                    <SearchBar toggleSearchForm={this.toggleSearchForm} />;

  return (
    <div className="row" >
      {jobSearch}
      {jobSearchResult}
    </div>
  );
  }
}
