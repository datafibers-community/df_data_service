import React from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';
import shouldUpdate from 'recompose/shouldUpdate';
import compose from 'recompose/compose';
import FlatButton from 'material-ui/FlatButton';
import ContentCreate from 'material-ui/svg-icons/content/reply';
import linkToRecord from 'admin-on-rest';
import translate from 'admin-on-rest';

const BackToTopicButton = ({
    basePath = '',
    label = 'Back to Topic',
    record = {},
    translate,
}) => (
    <FlatButton
        primary
        label={label}
        icon={<ContentCreate />}
        containerElement={<Link to={`${basePath}/${record.id}`} />}
        style={{ overflow: 'inherit' }}
    />
);

BackToTopicButton.propTypes = {
    basePath: PropTypes.string,
    label: PropTypes.string,
    record: PropTypes.object,
    translate: PropTypes.func.isRequired,
};


export default BackToTopicButton;