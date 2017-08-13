export default {
    pos: {
        search: '搜索',
        configuration: '设置',
        language: '语言',
        theme: {
            name: '主题',
            light: '明亮',
            dark: '暗黑',
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
                title: 'Welcome to admin-on-rest demo',
                subtitle: 'This is the admin of an imaginary poster shop. Fell free to explore and modify the data - it\'s local to your computer, and will reset each time you reload.',
                aor_button: 'Admin-on-rest website',
                demo_button: 'Source for this demo',
            },
        },
    },
    resources: {
        processor: {
            name: '全部 |||| 全部',
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
            name: '连接 |||| 连接',
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
            name: '转换 |||| 转换',
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
            name: '主题 |||| 主题',
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
        logs: {
            name: '日志 |||| 日志',
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
                accept: 'Accept',
                reject: 'Reject',
            },
            notification: {
                approved_success: 'Review approved',
                approved_error: 'Error: Review not approved',
                rejected_success: 'Review rejected',
                rejected_error: 'Error: Review not rejected',
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
