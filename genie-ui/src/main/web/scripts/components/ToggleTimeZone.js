import T from "prop-types";
import React from "react";
import Switch from "react-switch";

export default class ToggleTimeZone extends React.Component {
  static propTypes = {
    handleChange: T.func.isRequired,
    checked: T.bool.isRequired
  };

  constructor(props, context) {
    super(props, context);
  }

  render() {
    return (
      <Switch
        checked={this.props.checked}
        onChange={this.props.handleChange}
        uncheckedIcon={
          <div
            style={{
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              height: "100%",
              color: "white",
              fontSize: 15,
              paddingRight: 2
            }}
          >
            PST
          </div>
        }
        checkedIcon={
          <div
            style={{
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              height: "100%",
              color: "white",
              fontSize: 15,
              paddingRight: 2
            }}
          >
            UTC
          </div>
        }
        onColor="#B9090A"
        onHandleColor="#FFFFFF"
        handleDiameter={30}
        boxShadow="0px 1px 5px rgba(0, 0, 0, 0.6)"
        activeBoxShadow="0px 0px 1px 10px rgba(0, 0, 0, 0.2)"
        height={30}
        width={70}
        className="react-switch"
        id="material-switch"
      />
    );
  }
}
