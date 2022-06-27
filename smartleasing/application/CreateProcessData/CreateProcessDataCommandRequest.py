from pdip.cqrs.decorators import requestclass


@requestclass
class CreateProcessDataCommandRequest:
    RecreateLeasingCsv: bool = False
    RecreateBuildingCsv: bool = False

