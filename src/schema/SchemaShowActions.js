import React from 'react';
import { CardActions } from 'material-ui/Card';
import { EditButton, ListButton, DeleteButton, RefreshButton } from 'admin-on-rest';
import BackToTopicButton from './BackToTopicButton';

const cardActionStyle = {
    zIndex: 2,
    display: 'inline-block',
    float: 'right',
};

const SchemaShowActions = ({ basePath, data, hasDelete, hasShow, refresh }) => (
    <CardActions style={cardActionStyle}>
        <BackToTopicButton basePath={basePath} record={data} />
        <ListButton basePath={basePath} />
    </CardActions>
);

export default SchemaShowActions;
