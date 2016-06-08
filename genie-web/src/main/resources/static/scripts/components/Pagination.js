import React, { PropTypes as T } from 'react';

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
  page: T.shape({
    size          : T.number,
    totalElements : T.number,
    totalPages    : T.number,
    number        : T.number,
  }),
  pageType: T.string,
  links: T.shape({
    first : T.objectOf(T.string),
    self  : T.objectOf(T.string),
    last  : T.objectOf(T.string),
    prev  : T.objectOf(T.string),
    next  : T.objectOf(T.string),
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
  pageType : T.string,
  text     : T.string,
  url      : T.string,
};

export default Pagination;
