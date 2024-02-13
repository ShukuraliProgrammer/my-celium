import main


def test_all_jobs_scheduled():

    scheduler = main.schedule_all_jobs(start_scheduler=False)

    update_functions = [func for func in dir(main) if func.startswith("update")]

    for job in scheduler.get_jobs():
        assert job.id in update_functions, f"{job.id} should be scheduled but it's not"
