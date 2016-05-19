import React from 'react';
import { render } from 'react-dom';
import { Link } from 'react-router';

const SiteFooter = (props) =>
  <div className="site-footer">
    <footer>
      <p>{props.version}</p>
    </footer>
  </div>;

export default SiteFooter;
