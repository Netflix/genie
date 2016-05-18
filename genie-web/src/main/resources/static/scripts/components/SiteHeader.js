import React from 'react';
import { Link } from 'react-router';

const SiteHeader = (props) =>
  <div className="site-header">
    <nav className="navbar">
      <div className="navbar-header">
        <ul className="nav navbar-nav">
          {props.headers.map((header, index) =>
            <li key={index}><Link to={header.url} activeClassName={header.className}>{header.name}</Link></li>
          )}
        </ul>
      </div>
    </nav>
  </div>;

export default SiteHeader;
