import T from "prop-types";
import React from "react";
import { Link } from "react-router";

const InfoTable = props =>
  <table className="table" style={{ width: "50%" }}>
    <thead>
      <tr>
        <td>Id</td>
        <td>Name</td>
      </tr>
    </thead>
    <tbody>
      {props.data.map((info, index) =>
        <tr key={index}>
          <td>
            <Link to={`${props.type}?name=${info.name}&rowId=${info.id}`}>
              {info.id}
            </Link>
          </td>
          <td>
            {info.name}
          </td>
        </tr>
      )}
    </tbody>
  </table>;

InfoTable.propTypes = {
  type: T.string,
  data: T.arrayOf(T.shape({ id: T.string, name: T.string }))
};

export default InfoTable;
