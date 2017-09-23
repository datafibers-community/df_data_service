import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import IconButton from 'material-ui/IconButton';
import ThumbUp from 'material-ui/svg-icons/av/pause-circle-outline';
import ThumbDown from 'material-ui/svg-icons/av/play-circle-outline';
import ReplayIcon from 'material-ui/svg-icons/action/autorenew';
import { reviewApprove as reviewApproveAction, reviewReject as reviewRejectAction, reviewRestart as reviewRestartAction} from './reviewActions';
import { refreshView as refreshViewAction } from 'admin-on-rest';

class ApproveButton extends Component {
    handleApprove = () => {
        const { reviewApprove, record } = this.props;
        reviewApprove(record.id, record);
        this.props.refreshView();
    }

    handleReject = () => {
        const { reviewReject, record } = this.props;
        reviewReject(record.id, record);
        this.props.refreshView();
    }

    handleRestart = () => {
        const { reviewRestart, record } = this.props;
        reviewRestart(record.id, record);
        this.props.refreshView();
    }

    render() {
        const { record } = this.props;
        return (
        <span>
            {record && record.status === 'RUNNING' ? <IconButton onClick={this.handleApprove} ><ThumbUp color="#daa520" /> </IconButton>:
            record && record.status === 'PAUSED' ? <IconButton onClick={this.handleReject} ><ThumbDown color="#4CAF50" /> </IconButton>:""}
            <IconButton onClick={this.handleRestart} disabled={record.status === 'PAUSED'}><ReplayIcon color="#00bfff" /> </IconButton>
        </span>
        );
    }
}

ApproveButton.propTypes = {
    record: PropTypes.object,
    reviewApprove: PropTypes.func,
    reviewReject: PropTypes.func,
    reviewRestart: PropTypes.func,
};

export default connect(null, {
    reviewApprove: reviewApproveAction,
    reviewReject: reviewRejectAction,
    reviewRestart: reviewRestartAction,
    refreshView: refreshViewAction,
})(ApproveButton);
