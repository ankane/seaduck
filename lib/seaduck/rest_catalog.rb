module SeaDuck
  class RestCatalog < Catalog
    def initialize(uri:, warehouse: nil, default_namespace: "main", _secret_options: nil)
      attach_options = {
        endpoint: uri,
        authorization_type: "none"
      }
      _initialize(
        warehouse.to_s,
        default_namespace:,
        attach_options:,
        secret_options: _secret_options
      )
    end
  end
end
