import React from 'react';
import { CardActions } from 'material-ui/Card';
import { EditButton, ListButton, DeleteButton, RefreshButton } from 'admin-on-rest';
import PreviewDataButton from '../buttons/PreviewDataButton';

const cardActionStyle = {
    zIndex: 2,
    display: 'inline-block',
    float: 'right',
};

const SchemaShowActions = ({ basePath, data, hasDelete, hasShow, refresh }) => (
    <CardActions style={cardActionStyle}>
        <EditButton basePath={basePath} record={data} />
        <ListButton basePath={basePath} />
        <DeleteButton basePath={basePath} record={data} />
    </CardActions>
);

export default SchemaShowActions;
