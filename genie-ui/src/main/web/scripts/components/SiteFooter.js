import PropTypes from "prop-types";
import React from "react";

const SiteFooter = props =>
  <div className="site-footer">
    <footer>
      <p>
        {props.version}
      </p>
    </footer>
  </div>;

SiteFooter.propTypes = {
  version: PropTypes.string
};

export default SiteFooter;
