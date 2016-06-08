import React, { PropTypes as T } from 'react';
import { Link } from 'react-router';

const SiteHeader = (props) =>
  <div className="site-header">
    <nav className="navbar">
      <div className="container-fluid">
        <div className="navbar-header">
          <ul className="nav navbar-nav">
            {props.headers.map((header, index) =>
              <li key={index}><Link to={header.url} activeClassName={header.className}>{header.name}</Link></li>
            )}
          </ul>
        </div>
        <ul className="nav navbar-nav navbar-right site-header-info">
          {props.infos ? props.infos.map((info, index) =>
            <li key={index} className={info.className}><span>{info.name}</span></li>)
           : null
          }
        </ul>
      </div>
    </nav>
  </div>;

SiteHeader.propTypes = {
  headers : T.arrayOf(T.object),
  infos   : T.arrayOf(T.object),
};

export default SiteHeader;
