import operator
import pynetbox
import re
import requests

requests.packages.urllib3.disable_warnings()


class NetBox:
    def __init__(self, url, token):
        self.nb = pynetbox.api(
            url,
            token=token
        )

        self.nb_objects = {
            "tags": {
                "path": "extras.tags",
                "slug_generate": True,
                "required_fields": [
                    "name",
                ],
                "lookup_fields": [
                    "name"
                ],
            },
            "regions": {
                "path": "dcim.regions",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.region_normalization
            },
            "tenant_groups": {
                "path": "tenancy.tenant_groups",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.tenant_group_normalization
            },
            "tenants": {
                "path": "tenancy.tenants",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.tenant_normalization
            },
            "site_groups": {
                "path": "dcim.site_groups",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.site_group_normalization
            },
            "sites": {
                "path": "dcim.sites",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.site_normalization
            },
            "locations": {
                "path": "dcim.locations",
                "slug_generate": True,
                "required_fields": [
                    "name",
                    "site"
                ],
                "lookup_fields": [
                    "name",
                    "site_id"
                ],
                "normalization_fn": self.location_normalization
            },
            "racks": {
                "path": "dcim.racks",
                "required_fields": [
                    "name",
                    "site"
                ],
                "lookup_fields": [
                    "name",
                    "site_id"
                ],
                "normalization_fn": self.rack_normalization
            },
            "contact_roles": {
                "path": "tenancy.contact_roles",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
            },
            "contact_groups": {
                "path": "tenancy.contact_groups",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.contact_group_normalization
            },
            "contacts": {
                "path": "tenancy.contacts",
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.contact_normalization
            },
            "contact_assignments": {
                "path": "tenancy.contact_assignments",
                "required_fields": [
                    "content_type",
                    "object_id",
                    "contact",
                    "role",
                ],
                "lookup_fields": [
                    "content_type",
                    "object_id",
                    "contact_id",
                    "role_id",
                ],
                "normalization_fn": self.contact_assignment_normalization
            },
            "device_roles": {
                "path": "dcim.device_roles",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
            },
            "manufacturers": {
                "path": "dcim.manufacturers",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
            },
            "platforms": {
                "path": "dcim.platforms",
                "slug_generate": True,
                "required_fields": [
                    "name"
                ],
                "lookup_fields": [
                    "name"
                ],
                "normalization_fn": self.platform_normalization
            },
            "device_types": {
                "path": "dcim.device_types",
                "slug_generate": True,
                "required_fields": [
                    "model",
                    "manufacturer"
                ],
                "lookup_fields": [
                    "model",
                    "manufacturer_id"
                ],
                "normalization_fn": self.device_type_normalization
            },
            "vlans": {
                "path": "ipam.vlans",
                "required_fields": [
                    "vid",
                    "name",
                    "site"
                ],
                "lookup_fields": [
                    "vid",
                    "site_id"
                ],
                "normalization_fn": self.vlan_normalization,
                "create_if_absent": self.create_absent_vlan,
            },
            "devices": {
                "path": "dcim.devices",
                "required_fields": [
                    "name",
                    "role",
                    "device_type",
                    "site"
                ],
                "lookup_fields": [
                    "name",
                ],
                "normalization_fn": self.device_normalization
            },
            "inventory_items": {
                "path": "dcim.inventory-items",
                "required_fields": [
                    "device",
                    "name",
                    "serial",
                ],
                "lookup_fields": [
                    "device_id",
                    "name",
                    "serial",
                ],
                "normalization_fn": self.inventory_normalization
            },
            "interfaces": {
                "path": "dcim.interfaces",
                "required_fields": [
                    "name",
                    "device",
                ],
                "lookup_fields": [
                    "name",
                    "device_id",
                ],
                "normalization_fn": self.interface_normalization,
                "create_if_absent": self.create_absent_interface,
            },
            "ip_addresses": {
                "path": "ipam.ip-addresses",
                "required_fields": [
                    "address",
                ],
                "lookup_fields": [
                    "address",
                ],
            },
            "prefixes": {
                "path": "ipam.prefixes",
                "required_fields": [
                    "prefix",
                ],
                "lookup_fields": [
                    "prefix",
                ],
            }
        }

        self.nb_id_cache = {x: {} for x in self.nb_objects.keys()}

        self.interface_ip_list = []

    @staticmethod
    def nb_slug(raw_name):
        raw_name = raw_name.lower()
        raw_name = re.sub(r'[^\w\s-]', '', raw_name)
        raw_name = re.sub(r'[\s-]+', '-', raw_name)
        raw_name = raw_name.strip('-')
        return raw_name

    @staticmethod
    def nb_interface_type(interface_name: str):
        if "." in interface_name:
            return "virtual"
        elif interface_name.startswith("lo"):
            return "virtual"
        elif interface_name in ["vlan", "irb"]:
            return "virtual"
        elif interface_name.startswith("ge-"):
            return "1000base-t"
        elif interface_name.startswith("xe-"):
            return "10gbase-x-sfpp"
        elif interface_name.startswith("ae"):
            return "lag"
        else:
            return "other"

    def region_normalization(self, raw_data):
        region_parent = raw_data.get("parent")
        if region_parent:
            raw_data["parent"] = self.get_nb_id("regions", {"name": region_parent})
        return raw_data

    def tenant_group_normalization(self, raw_data):
        tenant_group_parent = raw_data.get("parent")
        if tenant_group_parent:
            raw_data["parent"] = self.get_nb_id("tenant_groups", {"name": tenant_group_parent})
        return raw_data

    def tenant_normalization(self, raw_data):
        tenant_group = raw_data.get("group")
        if tenant_group:
            raw_data["group"] = self.get_nb_id("tenant_groups", {"name": tenant_group})
        return raw_data

    def site_group_normalization(self, raw_data):
        site_parent = raw_data.get("parent")
        if site_parent:
            raw_data["parent"] = self.get_nb_id("site_groups", {"name": site_parent})
        return raw_data

    def site_normalization(self, raw_data):
        site_group = raw_data.get("group")
        if site_group:
            raw_data["group"] = self.get_nb_id("site_groups", {"name": site_group})
        site_region = raw_data.get("region")
        if site_region:
            raw_data["region"] = self.get_nb_id("regions", {"name": site_region})
        site_tenant = raw_data.get("tenant")
        if site_tenant:
            raw_data["tenant"] = self.get_nb_id("tenants", {"name": site_tenant})
        return raw_data

    def location_normalization(self, raw_data):
        location_parent = raw_data.get("parent")
        if location_parent:
            raw_data["parent"] = self.get_nb_id("locations", {"name": location_parent})
        location_site = raw_data.get("site")
        if location_site:
            raw_data["site"] = self.get_nb_id("sites", {"name": location_site})
        location_tenant = raw_data.get("tenant")
        if location_tenant:
            raw_data["tenant"] = self.get_nb_id("tenants", {"name": location_tenant})
        return raw_data

    def rack_normalization(self, raw_data):
        rack_location = raw_data.get("location")
        if rack_location:
            raw_data["location"] = self.get_nb_id("locations", {"name": rack_location})
        rack_site = raw_data.get("site")
        if rack_site:
            raw_data["site"] = self.get_nb_id("sites", {"name": rack_site})
        rack_tenant = raw_data.get("tenant")
        if rack_tenant:
            raw_data["tenant"] = self.get_nb_id("tenants", {"name": rack_tenant})
        return raw_data

    def contact_group_normalization(self, raw_data):
        region_parent = raw_data.get("parent")
        if region_parent:
            raw_data["parent"] = self.get_nb_id("contact_groups", {"name": region_parent})
        return raw_data

    def contact_normalization(self, raw_data):
        contact_group = raw_data.get("group")
        if contact_group:
            raw_data["group"] = self.get_nb_id("contact_groups", {"name": contact_group})
        return raw_data

    def contact_assignment_normalization(self, raw_data):
        contact = raw_data.get("contact")
        if contact:
            raw_data["contact"] = self.get_nb_id("contacts", {"name": contact})
        contact_role = raw_data.get("role")
        if contact_role:
            raw_data["role"] = self.get_nb_id("contact_roles", {"name": contact_role})
        object_id = raw_data.get("object_id")
        content_type = raw_data.get("content_type")
        if object_id and content_type:
            raw_data["object_id"] = self.get_nb_id(f"{content_type.split('.')[-1]}s", {"name": object_id})  # !!!!!
        return raw_data

    def platform_normalization(self, raw_data):
        platform_manufacturer = raw_data.get("manufacturer")
        if platform_manufacturer:
            raw_data["manufacturer"] = self.get_nb_id("manufacturers", {"name": platform_manufacturer})
        return raw_data

    def device_type_normalization(self, raw_data):
        device_type_manufacturer = raw_data.get("manufacturer")
        if device_type_manufacturer:
            raw_data["manufacturer"] = self.get_nb_id("manufacturers", {"name": device_type_manufacturer})
        return raw_data

    def vlan_normalization(self, raw_data):
        vlan_site = raw_data.get("site")
        if vlan_site:
            raw_data["site"] = self.get_nb_id("sites", {"name": vlan_site})
        return raw_data

    def device_normalization(self, raw_data):
        device_role = raw_data.get("role")
        if device_role:
            raw_data["role"] = self.get_nb_id("device_roles", {"name": device_role})
        device_type = raw_data.get("device_type")
        if device_type:
            raw_data["device_type"] = self.get_nb_id("device_types", {"model": device_type})
        platform = raw_data.get("platform")
        if platform:
            raw_data["platform"] = self.get_nb_id("platforms", {"name": platform})
        device_site = raw_data.get("site")
        if device_site:
            raw_data["site"] = self.get_nb_id("sites", {"name": device_site})
        device_location = raw_data.get("location")
        if device_location:
            raw_data["location"] = self.get_nb_id("locations", {"name": device_location})
        device_tenant = raw_data.get("tenant")
        if device_tenant:
            raw_data["tenant"] = self.get_nb_id("tenants", {"name": device_tenant})
        device_rack = raw_data.get("rack")
        if device_rack:
            raw_data["rack"] = self.get_nb_id("racks", {"name": device_rack})
        return raw_data

    def interface_normalization(self, raw_data):
        device = raw_data["device"]
        device_id = self.get_nb_id("devices", {"name": device})
        raw_data["device"] = device_id
        raw_data["type"] = self.nb_interface_type(raw_data["name"])

        lag = raw_data.get("lag")
        if lag:
            raw_data["lag"] = self.get_nb_id("interfaces", {"device_id": device_id, "name": lag, })

        parent = raw_data.get("parent")
        if parent:
            raw_data["parent"] = self.get_nb_id("interfaces", {"device_id": device_id, "name": parent, })

        description = raw_data.get("description")
        if description:
            raw_data["description"] = description

        mode = raw_data.get("mode")
        if mode and mode in ["access", "tagged"]:
            raw_data["mode"] = mode

            site_id = self.nb_id_cache["sites"].get(f"device={device}")
            if not site_id:
                site_id = self.get_object("devices", {"name": device}).site.id
                self.nb_id_cache["sites"][f"device={device}"] = site_id

            tagged_vlans = raw_data.get("tagged_vlans")
            if tagged_vlans:
                raw_data["tagged_vlans"] = [
                    self.get_nb_id("vlans", {"site_id": site_id, "vid": x}) for x in tagged_vlans
                ]
            untagged_vlan = raw_data.get("untagged_vlan")
            if untagged_vlan:
                raw_data["untagged_vlan"] = self.get_nb_id("vlans", {"site_id": site_id, "vid": untagged_vlan})

        ipv4_addresses = raw_data.get("ipv4")
        ipv6_addresses = raw_data.get("ipv6")
        if ipv4_addresses:
            self.interface_ip_list += ipv4_addresses
        if ipv6_addresses:
            self.interface_ip_list += ipv6_addresses

        return raw_data

    def inventory_normalization(self, raw_data):
        raw_data["device"] = self.get_nb_id("devices", {"name": raw_data["device"]})
        return raw_data

    def normalization(self, object_name, raw_data):
        object_conf = self.nb_objects[object_name]

        required_fields = object_conf["required_fields"]
        object_fields = raw_data.keys()
        required_check = list(set(required_fields) - set(object_fields))
        if required_check:
            raise Exception(f"Required fields: {required_check} omitted in {raw_data}")

        object_slug = object_conf.get("slug_generate")
        if object_slug:
            item_name = raw_data.get("name")
            item_model = raw_data.get("model")
            if item_name:
                raw_data["slug"] = self.nb_slug(item_name)
            elif item_model:
                raw_data["slug"] = self.nb_slug(item_model)
            else:
                raise Exception(f"Name is not defined for slug generation in {raw_data}")

        normalization_fn = object_conf.get("normalization_fn")
        if callable(normalization_fn):
            raw_data = normalization_fn(raw_data)

        tags = raw_data.get("tags")
        if tags:
            raw_data["tags"] = [self.get_object("tags", {"name": x}).id for x in tags]

        return raw_data

    def lookup(self, object_name, object_data):
        lookup_clause = dict()
        lookup_fields = self.nb_objects[object_name]["lookup_fields"]

        for lookup_field in lookup_fields:
            if lookup_field.endswith("_id") and lookup_field != "object_id":
                lookup_clause[lookup_field] = object_data[lookup_field.replace("_id", "")]
            else:
                lookup_clause[lookup_field] = object_data[lookup_field]
        return lookup_clause

    def create_update_object(self, object_name, object_data):
        normal_data = self.normalization(object_name, object_data)
        lookup_clause = self.lookup(object_name, normal_data)
        nb_object = self.get_object(object_name, lookup_clause)
        if nb_object:
            nb_object.update(normal_data)
            nb_object.save()
            print(f"UPDATE {normal_data}")
        else:
            nb_object = self.create_object(object_name, normal_data)
            print(f"CREATE {normal_data}")

        if object_name == "interfaces" and self.interface_ip_list:
            for ip_address in self.interface_ip_list:
                self.create_update_object("ip_addresses", {
                    "address": ip_address,
                    "assigned_object_type": "dcim.interface",
                    "assigned_object_id": nb_object.id
                })
            self.interface_ip_list = []

    def get_nb_id(self, object_name, lookup_clause):
        cache_key = "__".join([f"{x}={y}" for x, y in lookup_clause.items()])
        cache_value = self.nb_id_cache[object_name].get(cache_key)
        if cache_value:
            return cache_value
        else:
            get_nb_object = self.get_object(object_name, lookup_clause)
            create_absent_fn = self.nb_objects[object_name].get("create_if_absent")
            if not get_nb_object and callable(create_absent_fn):
                nb_id = create_absent_fn(lookup_clause).id
                self.nb_id_cache[object_name][cache_key] = nb_id
                return nb_id
            elif not get_nb_object:
                raise Exception(f"Unable to find {object_name}, with data: {lookup_clause}")
            else:
                nb_id = get_nb_object.id
                self.nb_id_cache[object_name][cache_key] = nb_id
                return nb_id

    def get_object(self, object_name, lookup_clause):
        object_path = self.nb_objects[object_name]["path"]
        return operator.attrgetter(object_path)(self.nb).get(**lookup_clause)

    def get_all(self, object_name):
        object_path = self.nb_objects[object_name]["path"]
        return operator.attrgetter(object_path)(self.nb).all()

    def create_object(self, object_name, object_data):
        object_path = self.nb_objects[object_name]["path"]
        return operator.attrgetter(object_path)(self.nb).create(object_data)

    def create_absent_vlan(self, vlan_data):
        print(f"CREATE {vlan_data}")
        create_data = {
            "name": f"VLAN{vlan_data['vid']}",
            "vid": vlan_data["vid"],
            "site": vlan_data["site_id"],
        }
        return self.create_object("vlans", create_data)

    def create_absent_interface(self, interface_data):
        print(f"CREATE {interface_data}")
        create_data = {
            "name": interface_data["name"],
            "device": interface_data["device_id"],
            "type": self.nb_interface_type(interface_data["name"]),
        }
        return self.create_object("interfaces", create_data)

    def delete_object(self, object_path, object_data):
        pass

    def filter_object(self, object_name, lookup_clause):
        object_path = self.nb_objects[object_name]["path"]
        return operator.attrgetter(object_path)(self.nb).filter(**lookup_clause)