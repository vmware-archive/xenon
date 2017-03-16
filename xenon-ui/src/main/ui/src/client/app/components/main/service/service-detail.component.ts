// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Location } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { URL } from '../../../frameworks/app/enums/index';
import { ModalContext, Node, QueryTask, ServiceDocument,
    ServiceDocumentQueryResult } from '../../../frameworks/app/interfaces/index';
import { ODataUtil, StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-service-detail',
    moduleId: module.id,
    templateUrl: './service-detail.component.html',
    styleUrls: ['./service-detail.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ServiceDetailComponent implements OnInit, OnDestroy {
    /**
     * Context object for rendering create child service modal.
     */
    createChildServiceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: '',
            body: ''
        }
    };

    /**
     * Links to all the available services.
     */
    private _serviceLinks: string[] = [];

    /**
     * Links to all the available child services within the specified service.
     */
    private _childServicesLinks: string[] = [];

    /**
     * Link to the next page of the child services
     */
    private _childServiceNextPageLink: string;

    /**
     * A lock to prevent the same next page link from being requested multiple times when scrolling
     */
    private _childServiceNextPageLinkRequestLocked: boolean = false;

    /**
     * Id for the selected service. E.g. /core/examples
     */
    private _selectedServiceId: string;

    /**
     * Id for the selected child service.
     */
    private _selectedChildServiceId: string;

    /**
     * Subscriptions to services.
     */
    private _activatedRouteParamsSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;
    private _baseServiceGetLinksSubscription: Subscription;
    private _baseServiceGetChildServiceListSubscription: Subscription;
    private _baseServiceGetChildServiceListNextPageSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService,
        private _activatedRoute: ActivatedRoute,
        private _router: Router,
        private _browserLocation: Location) {}

    ngOnInit(): void {
        // Update data when selected node changes
        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    // Navigate to the parent service grid when selected node changes
                    this._router.navigate(['/main/service'], {
                        relativeTo: this._activatedRoute,
                        queryParams: {
                            'node': this._activatedRoute.snapshot.queryParams['node']
                        }
                    });
                });

        this._activatedRouteParamsSubscription =
            this._activatedRoute.params.subscribe(
                (params: {[key: string]: any}) => {
                    this._selectedServiceId =
                        StringUtil.decodeFromId(params['id'] as string);

                    this._selectedChildServiceId = params['childId'];

                    // Set modal context
                    this.createChildServiceModalContext.name = this._selectedServiceId;
                    this.createChildServiceModalContext.data['documentSelfLink'] = this._selectedServiceId;
                    this.createChildServiceModalContext.data['body'] = '';

                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceGetLinksSubscription)) {
            this._baseServiceGetLinksSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceGetChildServiceListSubscription)) {
            this._baseServiceGetChildServiceListSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._activatedRouteParamsSubscription)) {
            this._activatedRouteParamsSubscription.unsubscribe();
        }
    }

    getServiceLinks(): string[] {
        return this._serviceLinks;
    }

    getChildServiceLinks(): string[] {
        return _.map(this._childServicesLinks, (childServiceLink: string) => {
            return StringUtil.parseDocumentLink(childServiceLink, this._selectedServiceId).id;
        });
    }

    getSelectedServiceId(): string {
        return this._selectedServiceId;
    }

    getSelectedServiceRouterId(id: string): string {
        return StringUtil.encodeToId(id);
    }

    getSelectedChildServiceId(): string {
        return this._selectedChildServiceId;
    }

    onCreateChildService(event: MouseEvent): void {
        var selectedServiceId: string = this.createChildServiceModalContext.data['documentSelfLink'];
        var body: string = this.createChildServiceModalContext.data['body'];

        if (!selectedServiceId || !body) {
            return;
        }

        this._baseService.post(selectedServiceId, body).subscribe(
            (document: ServiceDocument) => {
                this._notificationService.set([{
                    type: 'SUCCESS',
                    messages: [`Child Service ${document.documentSelfLink} Created`]
                }]);

                // Reset body
                this.createChildServiceModalContext.data['body'] = '';
            },
            (error) => {
                // TODO: Better error handling
                this._notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    onSelectChildService(event: MouseEvent, childServiceId: string): void {
        event.stopImmediatePropagation();

        // Manually update the url to represent the selected child service instead of
        // using router navigate, to prevent the page from flashing (thus all the child services
        // get reloaded and pagination get massed up), and offer better performance.
        var serviceId: string = this._activatedRoute.snapshot.params['id'];
        var basePath = this._browserLocation.path();

        if (_.isUndefined(this._selectedChildServiceId)) {
            this._browserLocation.replaceState(basePath.replace(serviceId, `${serviceId}/${childServiceId}`));
        } else {
            this._browserLocation.replaceState(basePath.replace(this._selectedChildServiceId, childServiceId));
        }

        this._selectedChildServiceId = childServiceId;
    }

    onLoadNextPage(): void {
        if (_.isUndefined(this._childServiceNextPageLink) || this._childServiceNextPageLinkRequestLocked) {
            return;
        }

        this._childServiceNextPageLinkRequestLocked = true;
        this._baseServiceGetChildServiceListNextPageSubscription =
            this._baseService.getDocument(this._childServiceNextPageLink, '', false).subscribe(
                (document: QueryTask) => {
                    if (_.isEmpty(document.results) || this._childServiceNextPageLink === document.results.nextPageLink) {
                        return;
                    }

                    this._childServiceNextPageLinkRequestLocked = false;
                    this._childServicesLinks = _.concat(this._childServicesLinks, document.results.documentLinks);

                    // NOTE: Need to use forwarding link here since the paginated data is stored on a particular node
                    if (document.results.nextPageLink) {
                        this._childServiceNextPageLink = this._baseService.getForwardingLink(document.results.nextPageLink);
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    private _getData(): void {
        // Only get _serviceLinks once
        if (_.isEmpty(this._serviceLinks)) {
            // Reset _childServicesLinks when the service itself changes
            this._childServicesLinks = [];

            this._baseServiceGetLinksSubscription =
                this._baseService.post(URL.Root, URL.RootPostBody).subscribe(
                    (document: ServiceDocumentQueryResult) => {
                        this._serviceLinks = _.sortBy(document.documentLinks);
                    },
                    (error) => {
                        // TODO: Better error handling
                        this._notificationService.set([{
                            type: 'ERROR',
                            messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                        }]);
                    });
        }

        this._baseServiceGetChildServiceListSubscription =
            this._baseService.getDocument(this._selectedServiceId, `${ODataUtil.limit()}&${ODataUtil.orderBy('documentSelfLink')}`).subscribe(
                (document: ServiceDocumentQueryResult) => {
                    this._childServicesLinks = document.documentLinks;

                    // NOTE: Need to use forwarding link here since the paginated data is stored on a particular node
                    if (document.nextPageLink) {
                        this._childServiceNextPageLink = this._baseService.getForwardingLink(document.nextPageLink);
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
