import 'babel-polyfill';
import React, { Component } from 'react';
import { Admin, Delete, Resource } from 'admin-on-rest';

import './App.css';

import authClient from './authClient';
import sagas from './sagas';
import themeReducer from './themeReducer';
import Login from './Login';
import Layout from './Layout';
import Menu from './Menu';
import { Dashboard } from './dashboard';
import customRoutes from './routes';
import translations from './i18n';

import { ConnectIcon, ConnectShow, ConnectList, ConnectCreate, ConnectEdit } from './connect';
import { TransformIcon, TransformShow, TransformList, TransformCreate, TransformEdit } from './transform';
import { SchemaIcon, SchemaCreate, SchemaEdit, SchemaShow, SchemaList } from './schema';
import { ProcessorIcon, ProcessorList, ProcessorShow } from './processor';
import { HistIcon, HistList } from './history';
import { LoggingIcon, LoggingList, LoggingShow } from './logging';
import { VisitorList, VisitorEdit, VisitorDelete, VisitorIcon } from './visitors';
import { CommandList, CommandEdit, CommandIcon } from './commands';
import { ProductList, ProductCreate, ProductEdit, ProductIcon } from './products';
import { CategoryList, CategoryEdit, CategoryIcon } from './categories';
import { ReviewList, ReviewEdit, ReviewIcon } from './reviews';

import restClient from './restClient';
import RichTextInput from 'aor-rich-text-input';

class App extends Component {

    render() {
        return (
            <Admin
                title="DataFibers Admin"
                restClient={restClient}
                customReducers={{ theme: themeReducer }}
                customSagas={sagas}
                customRoutes={customRoutes}
                authClient={authClient}
                loginPage={Login}
                appLayout={Layout}
                menu={Menu}
                messages={translations}
            >
                <Resource name="processor" options={{ label: 'All' }} icon={ProcessorIcon} list={ProcessorList} show={ProcessorShow} />
                <Resource name="ps" options={{ label: 'Connect' }} icon={ConnectIcon} list={ConnectList} create={ConnectCreate} edit={ConnectEdit} remove={Delete} show={ConnectShow} />
                <Resource name="tr" options={{ label: 'Transform' }} icon={TransformIcon} list={TransformList} edit={TransformEdit} create={TransformCreate} remove={Delete} show={TransformShow} />
                <Resource name="insight" options={{ label: 'Insight' }} icon={ConnectIcon} list={ConnectList} create={ConnectCreate} edit={ConnectEdit} remove={Delete} show={ConnectShow} />
                <Resource name="schema" options={{ label: 'Topic' }} icon={SchemaIcon} list={SchemaList} create={SchemaCreate} edit={SchemaEdit} remove={Delete} show={SchemaShow} />
                <Resource name="model" options={{ label: 'Model' }} icon={SchemaIcon} list={SchemaList} create={SchemaCreate} edit={SchemaEdit} remove={Delete} show={SchemaShow} />
                <Resource name="logs" options={{ label: 'Logging' }} icon={LoggingIcon} list={LoggingList} remove={Delete} show={LoggingShow} />
                <Resource name="hist" options={{ label: 'History' }} icon={HistIcon} list={HistList} />
                <Resource name="status" />
                <Resource name="s2t" />
                <Resource name="s2p" />
                <Resource name="avroconsumer" />
//                <Resource name="customers" list={VisitorList} edit={VisitorEdit} remove={VisitorDelete} icon={VisitorIcon}/>
//                <Resource name="commands" list={CommandList} edit={CommandEdit} remove={Delete} icon={CommandIcon} options={{ label: 'Orders' }}/>
//                <Resource name="products" list={ProductList} create={ProductCreate} edit={ProductEdit} remove={Delete} icon={ProductIcon} />
//                <Resource name="categories" list={CategoryList} edit={CategoryEdit} remove={Delete} icon={CategoryIcon} />
//                <Resource name="reviews" list={ReviewList} edit={ReviewEdit} icon={ReviewIcon} />
            </Admin>
        );
    }
}

export default App;
