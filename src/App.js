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
import { SchemaIcon, SchemaShow, SchemaList } from './schema';
import { ProcessorIcon, ProcessorList } from './processor';
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
                dashboard={Dashboard}
                loginPage={Login}
                appLayout={Layout}
                //menu={Menu}
                messages={translations}
            >
                <Resource name="ps" options={{ label: 'Connect' }} icon={ConnectIcon} list={ConnectList} create={ConnectCreate} edit={ConnectEdit} remove={Delete} show={ConnectShow} />
                <Resource name="tr" options={{ label: 'Transform' }} icon={TransformIcon} list={TransformList} edit={ConnectEdit} create={ConnectCreate} remove={Delete}  show={ConnectShow} />
                <Resource name="schema" options={{ label: 'Topic' }} icon={SchemaIcon} list={SchemaList} remove={Delete} show={SchemaShow} />
                <Resource name="status" />
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
