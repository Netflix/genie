import React from 'react';
import { render } from 'react-dom';
import { Link } from 'react-router';

const Pagination = (props) => {
  const start = (props.page.number * props.page.size) + 1;
  let end = (start + props.page.size) - 1;
  if (!props.links.next) {
    end = props.page.totalElements;
  }

  let pageLinks = [];
  if (props.links.first) {
    pageLinks.push(<li key="1"><JobLink url={props.links.first.href} text="&laquo;" /></li>);
  }
  if (props.links.prev) {
    pageLinks.push(<li key="2"><JobLink url={props.links.prev.href} text="Previous" /></li>);
  }
  if (props.links.next) {
    pageLinks.push(<li key="3"><JobLink url={props.links.next.href} text="Next" /></li>);
  }
  if (props.links.last) {
    pageLinks.push(<li key="4"><JobLink url={props.links.last.href} text="&raquo;" /></li>);
  }

  return (
    <div>
      <span>Showing {start} to {end} of {props.page.totalElements} entries</span>
      <nav>
        <ul className="pager">
          {pageLinks}
        </ul>
      </nav>
    </div>
  );
}

const JobLink = (props) => {
  const jobUrl = (genieLink) => `jobs?${genieLink.substring(genieLink.indexOf('?') + 1)}`;
  return (
    <Link
      to={jobUrl(props.url)}
      activeClassName="active"
    >
    {props.text}
    </Link>
  );
};

export default Pagination;
