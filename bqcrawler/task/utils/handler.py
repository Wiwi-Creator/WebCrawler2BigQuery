def mongodb_task_error_handler(task_detail: dict):
    errors = []
    task_detail["nDuplicated"] = 0
    for error in task_detail["writeErrors"]:
        if error["code"] == 11000:
            task_detail["nDuplicated"] += 1
        else:
            errors.append("{} - {}".format(error["errmsg"], error.get("errInfo", "No errorInfo")))

    return errors[:1]
