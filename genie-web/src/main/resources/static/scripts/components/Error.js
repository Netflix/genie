import React from 'react';

const Error = (props) =>
  <div className="col-md-8 no-data-found">
    <div><h4>{props.error.error}</h4></div>
    <code>
      <div>{props.error.status}</div>
      <div>{props.error.exception}</div>
      <div>{props.error.message}</div>
    </code>
  </div>;

export default Error;


