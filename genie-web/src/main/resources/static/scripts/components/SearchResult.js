import React from 'react';

import Pagination from './Pagination';
import TableHeader from './TableHeader';

const SearchResult = (props) =>
  <div className={props.showSearchForm ? "col-md-10" : "col-md-12"}>
    <table className="table">
      <props.TableHeader headers={props.headers} />
      <props.TableBody rows={props.data} />
    </table>
    <Pagination
      page={props.page}
      pageType={props.pageType}
      links={props.links}
    />
  </div>

export default SearchResult;
