import React, { PropTypes } from 'react';

import Pagination from './Pagination';
import TableHeader from './TableHeader';

const SearchResult = (props) =>
  <div className={props.showSearchForm ? 'col-md-10' : 'col-md-12'}>
    <div className="table-responsive">
    <table className="table">
      <props.TableHeader headers={props.headers} />
      <props.TableBody rows={props.data} />
    </table>
  </div>
    <Pagination
      page={props.page}
      pageType={props.pageType}
      links={props.links}
    />
  </div>;

SearchResult.propTypes = {
  showSearchForm : PropTypes.bool,
  data           : React.PropTypes.arrayOf(React.PropTypes.object),
  headers        : React.PropTypes.arrayOf(React.PropTypes.string),
  rows           : React.PropTypes.arrayOf(React.PropTypes.object),
  page           : PropTypes.object,
  pageType       : PropTypes.string,
  links          : PropTypes.object,
};
export default SearchResult;
