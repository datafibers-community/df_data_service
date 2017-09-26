import React from 'react';
import { CardActions } from 'material-ui/Card';
import { ShowButton, ListButton, DeleteButton, RefreshButton } from 'admin-on-rest';
import PreviewDataButton from '../buttons/PreviewDataButton';

const cardActionStyle = {
    zIndex: 2,
    display: 'inline-block',
    float: 'right',
};

const SchemaEditActions = ({ basePath, data, hasDelete, hasShow, refresh }) => (
    <CardActions style={cardActionStyle}>
        <PreviewDataButton basePath={basePath} label= "Preview Data" record={data} />
        <ListButton basePath={basePath} />
        <DeleteButton basePath={basePath} record={data} />
        <RefreshButton refresh={refresh} />
    </CardActions>
);

export default SchemaEditActions;
