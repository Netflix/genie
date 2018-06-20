import PropTypes from "prop-types";
import React from "react";

const SearchBar = props =>
  <div className="side-menu">
    <a href="javascript:void(0)" onClick={() => props.toggleSearchForm()}>
      <i className="fa fa-search fa-lg" aria-hidden="true" />
    </a>
  </div>;

SearchBar.propTypes = { toggleSearchForm: PropTypes.func.isRequired };

export default SearchBar;
