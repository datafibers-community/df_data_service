import React from 'react';
import { connect } from 'react-redux';
import compose from 'recompose/compose';
import SettingsIcon from 'material-ui/svg-icons/action/settings';
import LabelIcon from 'material-ui/svg-icons/action/label';
import { translate, DashboardMenuItem, MenuItemLink } from 'admin-on-rest';

import { ProcessorIcon } from './processor';
import { ConnectIcon } from './connect';
import { TransformIcon } from './transform';
import { SchemaIcon } from './schema';
import { ModelIcon } from './model';
import { HistIcon } from './history';
import { LoggingIcon } from './logging';

const items = [
    { name: 'processor', icon: <ProcessorIcon /> },
    { name: 'ps', icon: <ConnectIcon /> },
    { name: 'tr', icon: <TransformIcon /> },
    { name: 'schema', icon: <SchemaIcon /> },
    { name: 'ml', icon: <ModelIcon /> },
    { name: 'hist', icon: <HistIcon /> },
    { name: 'logs', icon: <LoggingIcon /> }
];

const styles = {
    main: {
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'flex-start',
        height: '100%',
    },
};

const Menu = ({ onMenuTap, translate, logout }) => (
    <div style={styles.main}>
        <DashboardMenuItem onClick={onMenuTap} />
        {items.map(item => (
            <MenuItemLink
                key={item.name}
                to={`/${item.name}`}
                primaryText={translate(`resources.${item.name}.name`, { smart_count: 2 })}
                leftIcon={item.icon}
                onClick={onMenuTap}
            />
        ))}
        <MenuItemLink
            to="/configuration"
            primaryText={translate('pos.configuration')}
            leftIcon={<SettingsIcon />}
            onClick={onMenuTap}
        />
        {logout}
    </div>
);

const enhance = compose(
    connect(state => ({
        theme: state.theme,
        locale: state.locale,
    })),
    translate,
);

export default enhance(Menu);
