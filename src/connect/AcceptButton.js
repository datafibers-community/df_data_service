import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import FlatButton from 'material-ui/FlatButton';
import ThumbUp from 'material-ui/svg-icons/av/pause-circle-outline';
import { translate } from 'admin-on-rest';
import compose from 'recompose/compose';
import { reviewApprove as reviewApproveAction } from './reviewActions';

class AcceptButton extends Component {
    handleApprove = () => {
        const { reviewApprove, record } = this.props;
        reviewApprove(record.id, record);
    }

    render() {
        const { record, translate } = this.props;
        return record && record.status !== 'PAUSED' ? <FlatButton
            primary
            label={translate('resources.reviews.action.accept')}
            onClick={this.handleApprove}
            icon={<ThumbUp color="#00bfff" />}
        /> : <span/>;
    }
}

AcceptButton.propTypes = {
    record: PropTypes.object,
    reviewApprove: PropTypes.func,
    translate: PropTypes.func,
};

const enhance = compose(
    translate,
    connect(null, {
        reviewApprove: reviewApproveAction,
    })
);

export default enhance(AcceptButton);
