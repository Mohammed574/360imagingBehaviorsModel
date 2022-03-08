with select_attributes as
(
    select 
    distinct Courier_ID,
    CourierStatus,
    OrderStatus,
    GuideStatus,
    ModelStatus,
    RadiologyService,
    ConversionService,
    PrintService,
    TreatmentService,
    GuideService,
    AnatomicCase,
    WaxupDesignStatus,
    IsRush



    from {{ref('stg_order')}}
) select * from select_attributes
