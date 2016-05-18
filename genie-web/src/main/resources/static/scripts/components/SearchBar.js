import React from 'react';
import { render } from 'react-dom';

const SearchBar = (props) =>
  <div className="side-menu">
    <a href="javascript:void(0)" onClick={() => props.toggleSearchForm()}>
      <i className="fa fa-search fa-lg" aria-hidden="true"></i>
    </a>
  </div>;

export default SearchBar;
