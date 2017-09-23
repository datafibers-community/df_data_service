import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import FlatButton from 'material-ui/FlatButton';
import ReplayIcon from 'material-ui/svg-icons/action/autorenew';
import { translate } from 'admin-on-rest';
import compose from 'recompose/compose';
import { reviewRestart as reviewRestartAction } from './reviewActions';

class RestartButton extends Component {
    handleRestart = () => {
        const { reviewRestart, record } = this.props;
        reviewRestart(record.id, record);
    }

    render() {
        const { record, translate } = this.props;
        return record && record.status === 'RUNNING' ? <FlatButton
            primary
            label={translate('resources.reviews.action.restart')}
            onClick={this.handleRestart}
            icon={<ReplayIcon color="#daa520" />}
        /> : <span/>;
    }
}

RestartButton.propTypes = {
    record: PropTypes.object,
    reviewRestart: PropTypes.func,
    translate: PropTypes.func,
};

const enhance = compose(
    translate,
    connect(null, {
        reviewRestart: reviewRestartAction,
    })
);

export default enhance(RestartButton);
