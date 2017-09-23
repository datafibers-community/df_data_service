import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import IconButton from 'material-ui/IconButton';
import ThumbUp from 'material-ui/svg-icons/av/pause-circle-outline';
import ThumbDown from 'material-ui/svg-icons/av/play-circle-outline';
import { reviewApprove as reviewApproveAction, reviewReject as reviewRejectAction } from './reviewActions';
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

    render() {
        const { record } = this.props;
        return (
            <span>
                <IconButton onClick={this.handleApprove} disabled={record.status !== 'RUNNING'}><ThumbUp color="#00bcd4" /></IconButton>
                <IconButton onClick={this.handleReject} disabled={record.status !== 'PAUSED'}><ThumbDown color="#00bcd4" /></IconButton>
            </span>
        );
    }
}

ApproveButton.propTypes = {
    record: PropTypes.object,
    reviewApprove: PropTypes.func,
    reviewReject: PropTypes.func,
};

export default connect(null, {
    reviewApprove: reviewApproveAction,
    reviewReject: reviewRejectAction,
    refreshView: refreshViewAction,
})(ApproveButton);
