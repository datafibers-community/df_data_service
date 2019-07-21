export default {
    pos: {
        search: 'Search',
        configuration: 'Configuration',
        language: 'Language',
        theme: {
            name: 'Theme',
            light: 'Light',
            dark: 'Dark',
        },
        dashboard: {
            monthly_revenue: 'Monthly Revenue',
            new_orders: 'New Orders',
            pending_reviews: 'Pending Reviews',
            new_customers: 'New Customers',
            pending_orders: 'Pending Orders',
            order: {
                items: 'by %{customer_name}, one item |||| by %{customer_name}, %{nb_items} items',
            },
            welcome: {
                title: 'Welcome to DataFibers Admin',
                subtitle: 'This is the web admin interface DataFibers.',
                aor_button: 'DataFibers Website',
                demo_button: 'DataFibers GitHub',
            },
        },
    },
    resources: {
        processor: {
            name: 'All |||| All',
                    fields: {
                        commands: 'Orders',
                        groups: 'Segments',
                        last_seen_gte: 'Visited Since',
                        name: 'Name',
                    },
                    tabs: {
                        identity: 'Identity',
                        address: 'Address',
                        orders: 'Orders',
                        reviews: 'Reviews',
                        stats: 'Stats',
                    },
                    page: {
                        delete: 'Delete Customer',
                    },

        },
        ps: {
            name: 'Connect |||| Connect',
                    fields: {
                        commands: 'Orders',
                        groups: 'Segments',
                        last_seen_gte: 'Visited Since',
                        name: 'Name',
                    },
                    tabs: {
                        identity: 'Identity',
                        address: 'Address',
                        orders: 'Orders',
                        reviews: 'Reviews',
                        stats: 'Stats',
                    },
                    page: {
                        delete: 'Delete Customer',
                    },

        },
        tr: {
            name: 'Transform |||| Transform',
                    fields: {
                        commands: 'Orders',
                        groups: 'Segments',
                        last_seen_gte: 'Visited Since',
                        name: 'Name',
                    },
                    tabs: {
                        identity: 'Identity',
                        address: 'Address',
                        orders: 'Orders',
                        reviews: 'Reviews',
                        stats: 'Stats',
                    },
                    page: {
                        delete: 'Delete Customer',
                    },

        },
        insight: {
                    name: 'Insight |||| Insight',
                            fields: {
                                commands: 'Orders',
                                groups: 'Segments',
                                last_seen_gte: 'Visited Since',
                                name: 'Name',
                            },
                            tabs: {
                                identity: 'Identity',
                                address: 'Address',
                                orders: 'Orders',
                                reviews: 'Reviews',
                                stats: 'Stats',
                            },
                            page: {
                                delete: 'Delete Customer',
                            },

        },
        schema: {
            name: 'Topic |||| Topic',
                    fields: {
                        commands: 'Orders',
                        groups: 'Segments',
                        last_seen_gte: 'Visited Since',
                        name: 'Name',
                    },
                    tabs: {
                        identity: 'Identity',
                        address: 'Address',
                        orders: 'Orders',
                        reviews: 'Reviews',
                        stats: 'Stats',
                    },
                    page: {
                        delete: 'Delete Customer',
                    },
                    card: {
                        title: 'Please well understand the schema compatibility before editing.'
                    },

        },
        ml: {
                    name: 'Model |||| Model',
                            fields: {
                                commands: 'Orders',
                                groups: 'Segments',
                                last_seen_gte: 'Visited Since',
                                name: 'Name',
                            },
                            tabs: {
                                identity: 'Identity',
                                address: 'Address',
                                orders: 'Orders',
                                reviews: 'Reviews',
                                stats: 'Stats',
                            },
                            page: {
                                delete: 'Delete Customer',
                            },
                            card: {
                                title: 'Please well understand the schema compatibility before editing.'
                            },

        },
        logs: {
             name: 'Logging |||| Logging',
                     fields: {
                         commands: 'Orders',
                         groups: 'Segments',
                         last_seen_gte: 'Visited Since',
                         name: 'Name',
                     },
                     tabs: {
                         identity: 'Identity',
                         address: 'Address',
                         orders: 'Orders',
                         reviews: 'Reviews',
                         stats: 'Stats',
                     },
                     page: {
                         delete: 'Delete Customer',
                     },

        },
        hist: {
             name: 'History |||| History',
                     fields: {
                         commands: 'Orders',
                         groups: 'Segments',
                         last_seen_gte: 'Visited Since',
                         name: 'Name',
                     },
                     tabs: {
                         identity: 'Identity',
                         address: 'Address',
                         orders: 'Orders',
                         reviews: 'Reviews',
                         stats: 'Stats',
                     },
                     page: {
                         delete: 'Delete Customer',
                     },

         },
        customers: {
            name: 'Customer |||| Customers',
            fields: {
                commands: 'Orders',
                groups: 'Segments',
                last_seen_gte: 'Visited Since',
                name: 'Name',
            },
            tabs: {
                identity: 'Identity',
                address: 'Address',
                orders: 'Orders',
                reviews: 'Reviews',
                stats: 'Stats',
            },
            page: {
                delete: 'Delete Customer',
            },

        },
        commands: {
            name: 'Order |||| Orders',
            fields: {
                basket: {
                    delivery: 'Delivery',
                    reference: 'Reference',
                    quantity: 'Quantity',
                    sum: 'Sum',
                    tax_rate: 'Tax Rate',
                    total: 'Total',
                    unit_price: 'Unit Price',
                },
                customer_id: 'Customer',
                date_gte: 'Passed Since',
                date_lte: 'Passed Before',
                total_gte: 'Min amount',
            },
        },
        products: {
            name: 'Poster |||| Posters',
            fields: {
                category_id: 'Category',
                height_gte: 'Min height',
                height_lte: 'Max height',
                height: 'Height',
                image: 'Image',
                price: 'Price',
                reference: 'Reference',
                stock_lte: 'Low Stock',
                stock: 'Stock',
                thumbnail: 'Thumbnail',
                width_gte: 'Min width',
                width_lte: 'mx_width',
                width: 'Width',
            },
            tabs: {
                image: 'Image',
                details: 'Details',
                description: 'Description',
                reviews: 'Reviews',
            },
        },
        categories: {
            name: 'Category |||| Categories',
            fields: {
                products: 'Products',
            },

        },
        reviews: {
            name: 'Review |||| Reviews',
            fields: {
                customer_id: 'Customer',
                command_id: 'Order',
                product_id: 'Product',
                date_gte: 'Posted since',
                date_lte: 'Posted before',
                date: 'Date',
                comment: 'Comment',
                rating: 'Rating',
            },
            action: {
                accept: 'Pause',
                reject: 'Resume',
                restart: 'Restart',
            },
            notification: {
                approved_success: 'Task Paused',
                approved_error: 'Error: Task not Paused',
                rejected_success: 'Task Resumed',
                rejected_error: 'Error: Task not Resumed',
            },
        },
        segments: {
            name: 'Segments',
            fields: {
                customers: 'Customers',
                name: 'Name',
            },
            data: {
                compulsive: 'Compulsive',
                collector: 'Collector',
                ordered_once: 'Ordered once',
                regular: 'Regular',
                returns: 'Returns',
                reviewer: 'Reviewer',
            },
        },
    },
};
