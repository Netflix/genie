import T from "prop-types";
import React from "react";

const Error = props =>
  <div className="col-md-10 result-panel-msg">
    <div>
      <h4>
        {props.error.error}
      </h4>
    </div>
    <code>
      <div>
        {props.error.status}
      </div>
      <div>
        {props.error.exception}
      </div>
      <div>
        {props.error.message}
      </div>
    </code>
  </div>;

Error.propTypes = { error: T.object };

export default Error;
