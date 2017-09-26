import React from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';
import shouldUpdate from 'recompose/shouldUpdate';
import compose from 'recompose/compose';
import FlatButton from 'material-ui/FlatButton';
import IconImage from 'material-ui/svg-icons/av/library-books';
import linkToRecord from 'admin-on-rest';
import translate from 'admin-on-rest';

const PreviewDataButton = ({
    basePath = '',
    record = {},
}) => (
    <FlatButton
        primary
        label="Preview Data"
        icon={<IconImage />}
        containerElement={
        <Link to={`${basePath}/${record.id}/show`} />
        }
        style={{ overflow: 'inherit' }}
    />
);

PreviewDataButton.propTypes = {
    basePath: PropTypes.string,
    record: PropTypes.object,
};

export default PreviewDataButton;