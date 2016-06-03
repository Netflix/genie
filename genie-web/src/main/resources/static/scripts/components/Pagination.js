import React, { PropTypes } from 'react';

import { Link } from 'react-router';

const Pagination = (props) => {
  const start = (props.page.number * props.page.size) + 1;
  let end = (start + props.page.size) - 1;
  if (!props.links.next) {
    end = props.page.totalElements;
  }

  let pageLinks = [];
  if (props.links.first) {
    pageLinks.push(
      <li key="1">
        <PageLink
          pageType={props.pageType}
          url={props.links.first.href}
          text="&laquo;"
        />
      </li>);
  }
  if (props.links.prev) {
    pageLinks.push(
      <li key="2">
        <PageLink
          pageType={props.pageType}
          url={props.links.prev.href}
          text="&larr; Previous"
        />
      </li>);
  }
  if (props.links.next) {
    pageLinks.push(
      <li key="3">
        <PageLink
          pageType={props.pageType}
          url={props.links.next.href}
          text="Next &rarr;"
        />
      </li>);
  }
  if (props.links.last) {
    pageLinks.push(
      <li key="4">
        <PageLink
          pageType={props.pageType}
          url={props.links.last.href}
          text="&raquo;"
        />
      </li>);
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
};

Pagination.propTypes = {
  page: PropTypes.shape({
    size          : PropTypes.number,
    totalElements : PropTypes.number,
    totalPages    : PropTypes.number,
    number        : PropTypes.number,
  }),
  pageType: PropTypes.string,
  links: PropTypes.shape({
    first : PropTypes.objectOf(PropTypes.string),
    self  : PropTypes.objectOf(PropTypes.string),
    last  : PropTypes.objectOf(PropTypes.string),
    prev  : PropTypes.objectOf(PropTypes.string),
    next  : PropTypes.objectOf(PropTypes.string),
  }),
};

const PageLink = (props) => {
  const constructUrl = (pageType, genieLink) => `${pageType}?${genieLink.substring(genieLink.indexOf('?') + 1)}`;
  return (
    <Link
      to={constructUrl(props.pageType, props.url)}
      activeClassName="active"
    >
    {props.text}
    </Link>
  );
};

PageLink.propTypes = {
  pageType : PropTypes.string,
  text     : PropTypes.string,
  url      : PropTypes.string,
};

export default Pagination;
