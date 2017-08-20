import React from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';
import IconButton from 'material-ui/IconButton';
import {cyan500} from 'material-ui/styles/colors';
import ContentCreate from 'material-ui/svg-icons/image/remove-red-eye';

const ShowProcessorButton = ({ basePath = '', record = {} }) => (
    <IconButton
        containerElement={<Link to={`/processor/${record.id}/show`} />}
        style={{ overflow: 'inherit' }}
    >
        <ContentCreate color={cyan500} />
    </IconButton>
);

ShowProcessorButton.propTypes = {
    basePath: PropTypes.string,
    record: PropTypes.object,
};

ShowProcessorButton.defaultProps = {
    style: { padding: 0 },
};

export default ShowProcessorButton;
