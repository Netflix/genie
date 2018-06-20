import React from "react";
import { fetch } from "../utils";
import Modal from "react-modal";

export default class AuthCheck extends React.Component {
  constructor(props) {
    super(props);
    this.state = { modalIsOpen: false };
  }

  componentDidMount() {
    // Every 15 mins
    this.authCheckTimer = setInterval(this.doAuthCheck, 900000);
  }

  componentWillUnmount() {
    clearInterval(this.authCheckTimer);
  }

  reloadPage = () => window.location.reload();

  closeModal = () => {
    this.setState({ modalIsOpen: false });
    this.reloadPage();
  };

  doAuthCheck = () => {
    fetch("/api/v3/jobs").fail(xhr => {
      if (xhr.status === 401) this.reloadPage();
      else this.setState({ modalIsOpen: true });
    });
  };

  get customStyles() {
    return {
      content: {
        padding: "5px",
        width: "40%",
        height: "190px",
        top: "40%",
        left: "50%",
        right: "auto",
        bottom: "auto",
        marginRight: "-50%",
        transform: "translate(-50%, -50%)"
      }
    };
  }

  render() {
    return (
      <Modal isOpen={this.state.modalIsOpen} style={this.customStyles}>
        <div>
          <div className="modal-header">
            <h4 className="modal-title" id="alert-modal-label">
              You have been logged out
            </h4>
          </div>
          <div className="modal-body">
            <div>
              Please click below or refresh your browser to log back in.
            </div>
          </div>
          <div className="modal-footer">
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => this.closeModal()}
            >
              Refresh and log me back in
            </button>
          </div>
        </div>
      </Modal>
    );
  }
}
